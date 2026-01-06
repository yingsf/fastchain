from __future__ import annotations

import asyncio
import json
from typing import Optional, override

from beanie import init_beanie
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import ValidationError
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure

from .config import MongoConfig
from .registry import ModelRegistry
from ....core.config import SettingsManager
from ....core.config.constants import ResourceName, ResourceKind
from ....core.resources.base import Resource, ResourceHealth, HealthStatus
from ....core.resources.events import Event, EventBus, EventConstants
from ....core.resources.limits import ResourceLimiter
from ....core.resources.watcher import BackgroundWatcher


class MongoHealthWatcher(BackgroundWatcher):
    """MongoDB 主动健康检查观察者

    该类继承自 BackgroundWatcher，专门负责定期对 MongoDB 连接进行主动健康检查
    通过定期执行 ping 命令，可以及时发现连接异常或网络抖动，触发自动重连或告警机制

    增强特性：
    - 集成 ResourceLimiter：支持可选的资源限流
    - 集成 EventBus：支持向上传递事件总线，激活基类的可观测性能力

    设计考虑：
    - 使用可配置的检查间隔（min_interval 和 max_interval），支持动态调整以适应不同环境的网络状况
    - 采用主动探测而非被动等待首次失败，可更早发现潜在问题，减少服务中断时间
    - 通过 BackgroundWatcher 的异步调度机制，避免阻塞主业务逻辑

    Attributes:
        self._manager (MongoManager): 所关联的 MongoDB 管理器实例，用于获取活跃连接和目标数据库名称。

    Methods:
        tick: 执行一次健康检查的核心逻辑。
    """

    def __init__(
        self,
        manager: MongoManager,
        event_bus: EventBus | None = None,
        limiter: ResourceLimiter | None = None
    ):
        """初始化 MongoDB 健康检查观察者

        Args:
            manager (MongoManager): 所关联的 MongoManager 实例
            event_bus (EventBus | None): 事件总线，用于基类发布内部事件
            limiter (ResourceLimiter | None): 资源限流器，用于控制检查频率（可选）
        """
        # 调用父类构造函数，设置观察者名称、日志记录器及增强组件
        # 将 event_bus 和 limiter 传递给基类，确保 BackgroundWatcher 能够利用框架能力
        super().__init__(
            name=f"{manager.name}-health",
            logger=logger,
            min_interval=60.0,
            max_interval=120.0,
            event_bus=event_bus,
            limiter=limiter
        )
        self._manager = manager

    @override
    async def tick(self) -> None:
        """执行一次健康检查

        该方法会获取当前活跃的 MongoDB 客户端，并向目标数据库发送 ping 命令
        若 ping 失败，会抛出异常（由 BackgroundWatcher 的异常处理机制捕获并记录）

        设计说明：
        - 使用 ping 命令是 MongoDB 官方推荐的轻量级健康检查方式，开销极小
        - 若客户端尚未初始化（client 为 None），则跳过本次检查，避免在启动阶段产生噪音日志
        - 异常会被 BackgroundWatcher 基类捕获，并根据配置决定是否触发告警或重启逻辑

        Raises:
            pymongo.errors.PyMongoError: 当 MongoDB 连接异常或 ping 命令失败时抛出
        """
        # 获取当前活跃的 MongoDB 客户端实例
        client = self._manager.get_active_client()
        if not client:
            # 客户端尚未初始化，跳过本次健康检查
            return

        # 获取目标数据库名称（从当前配置中读取）
        db_name = self._manager.get_target_database_name()

        # 向目标数据库发送 ping 命令，验证连接健康状态
        # 若连接断开或网络不可达，会抛出 ConnectionFailure 或 ServerSelectionTimeoutError
        await client.get_database(db_name).command("ping")


class MongoManager(Resource):
    """MongoDB 资源管理器

    该类负责管理 MongoDB 连接的完整生命周期，包括初始化、配置热重载、健康检查和优雅关闭
    作为系统核心资源之一，MongoManager 实现了与配置中心（如 Apollo）的集成，支持动态更新连接参数，并通过事件总线订阅配置变更事件

    核心职责：
    - 从配置中心加载 MongoDB 连接参数（URI、数据库名、连接池大小等）
    - 初始化并维护 Motor 异步客户端实例，支持连接池复用
    - 实现配置热重载：当检测到配置变更时，自动重新建立连接（仅当参数真正变化时）
    - 集成 Beanie ODM，自动发现并注册 MongoDB 文档模型
    - 提供主动健康检查机制，定期验证连接可用性
    - 在启动和运行时采用不同的错误处理策略（启动时快速失败，运行时容错降级）

    设计考虑：
    - 使用 asyncio.Lock 保护配置重载过程，防止并发重载导致的资源泄漏或状态不一致
    - 通过比对配置哈希值（model_dump）避免无效重载，减少不必要地连接重建开销
    - 健康检查间隔可动态调整，适应不同环境的网络延迟和可靠性要求
    - 在关闭旧客户端前先创建并测试新客户端，实现平滑切换，最小化服务中断窗口

    Attributes:
        self._settings (SettingsManager): 配置管理器，用于读取和订阅配置变更
        self._event_bus (EventBus): 事件总线，用于订阅配置更新事件
        self._client (AsyncIOMotorClient | None): 当前活跃的 MongoDB 客户端实例
        self._config (MongoConfig | None): 当前生效的 MongoDB 配置对象
        self._reload_lock (asyncio.Lock): 配置重载锁，防止并发重载导致竞态条件
        self._started (bool): 标记管理器是否已启动，用于区分启动阶段和运行阶段的错误处理策略
        self._health_watcher (MongoHealthWatcher): 健康检查观察者实例

    Methods:
        start: 启动 MongoDB 管理器，初始化连接并订阅配置事件
        stop: 停止 MongoDB 管理器，关闭连接和健康检查
        reload: 重新加载配置并重建连接（若配置有变化）
        health_check: 执行一次健康检查并返回健康状态
        get_active_client: 获取当前活跃的客户端实例（可能为 None）
        get_target_database_name: 获取目标数据库名称
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus):
        """初始化 MongoDB 资源管理器

        Args:
            settings (SettingsManager): 配置管理器实例，用于读取配置和订阅变更事件
            event_bus (EventBus): 事件总线实例，用于订阅和发布系统事件
        """
        # 调用父类构造函数，设置资源名称、类型和启动优先级
        super().__init__(
            name=ResourceName.MONGO_DB,
            kind=ResourceKind.DOCUMENT,
            startup_priority=10
        )
        self._settings = settings
        self._event_bus = event_bus

        # 初始化内部状态
        self._client: AsyncIOMotorClient | None = None
        self._config: MongoConfig | None = None

        # 创建异步锁，用于保护 reload() 方法的并发执行
        self._reload_lock = asyncio.Lock()

        self._started = False

        # 创建健康检查观察者实例
        # 注入 event_bus，使 Watcher 能够利用框架的事件能力
        # 虽然目前没有为 health_watcher 配置专门的 limiter，但保留接口一致性便于未来扩展
        self._health_watcher = MongoHealthWatcher(
            manager=self,
            event_bus=self._event_bus
        )

    async def start(self) -> None:
        """启动 MongoDB 管理器

        该方法会执行以下操作：
        1. 订阅配置中心的配置更新事件
        2. 加载初始配置并建立 MongoDB 连接
        3. 启动后台健康检查观察者

        设计说明：
        - 若已启动则跳过，避免重复初始化
        - 在启动阶段，若配置加载或连接失败会立即抛出异常（快速失败），确保系统不会带病运行
        - 启动成功后，_started 标志会被设为 True，后续的配置重载会采用容错策略

        Raises:
            ValueError: 当初始配置无效或缺失时抛出。
            ConnectionFailure: 当无法连接到 MongoDB 服务器时抛出。
            ServerSelectionTimeoutError: 当连接超时时抛出。
            OperationFailure: 当认证失败或数据库操作失败时抛出。
        """
        if self._started:
            # 已启动，跳过重复初始化
            return

        logger.info("正在启动 MongoManager...")

        # 订阅配置中心的配置更新事件
        self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        try:
            # 加载初始配置并建立连接，若失败会抛出异常，阻止管理器启动
            await self.reload()
        except Exception:
            logger.critical("MongoManager 启动失败")
            raise

        # 启动后台健康检查观察者
        # 观察者会按配置的时间间隔定期执行 ping 命令，监控连接健康状态
        await self._health_watcher.start()

        # 标记为已启动
        self._started = True

    async def stop(self) -> None:
        """停止 MongoDB 管理器

        该方法会执行以下操作：
        1. 停止后台健康检查观察者
        2. 关闭 MongoDB 客户端连接
        3. 清理内部状态

        设计说明：
        - 确保优雅关闭，避免连接泄漏或资源未释放
        - 停止顺序：先停止健康检查（避免在关闭过程中触发误报），再关闭客户端
        """
        logger.info("正在停止 MongoManager...")

        # 停止后台健康检查观察者
        await self._health_watcher.stop()

        if self._client:
            # 关闭 MongoDB 客户端连接
            self._client.close()
            self._client = None

        self._started = False

    async def _on_config_updated(self, _event: Event) -> None:
        """配置更新事件回调

        当配置中心推送新配置时，该方法会被事件总线调用
        方法会检查 MongoDB 相关配置是否变更，若有变化则触发热重载

        设计说明：
        - 仅在已启动状态下才执行重载，避免在启动早期阶段收到配置变更事件时产生竞态条件
        - 在运行时若重载失败，会记录异常但不会中断服务（容错设计）
        - 通过 reload() 内部的锁机制和配置比对，避免无效重载和并发重载问题

        Args:
            _event (Event): 配置更新事件对象（本方法当前未使用事件详情）
        """
        if not self._started:
            # 管理器尚未启动，跳过配置重载
            return

        logger.debug("检测到配置更新，检查是否需要重新加载 Mongo 配置...")
        try:
            # 尝试重新加载配置并重建连接（若配置确有变化）
            await self.reload()
        except Exception:
            logger.exception("MongoManager 热重载失败")

    async def reload(self) -> None:
        """重新加载配置并重建 MongoDB 连接（若配置有变化）

        该方法是配置热重载的核心逻辑，执行以下步骤：
        1. 加锁，防止并发重载
        2. 从配置中心读取最新的 MongoDB 配置
        3. 刷新健康检查间隔（根据最新配置动态调整）
        4. 比对新旧配置，若完全一致则跳过重建
        5. 若配置有变化，创建新客户端并测试连接
        6. 连接成功后，替换旧客户端并关闭旧连接

        设计考虑：
        - 使用 asyncio.Lock 确保同一时刻只有一个 reload 流程在执行，避免资源泄漏
        - 通过配置对象的 model_dump() 比对哈希值，避免配置未变更时的无效重载
        - 采用"先建后删"策略：先创建并验证新连接，成功后再关闭旧连接，最小化服务中断时间
        - 在启动阶段（_started=False），若配置无效或连接失败会抛出异常（快速失败）
        - 在运行时（_started=True），若配置无效会记录错误但不抛异常，保持当前连接继续运行（容错）

        Raises:
            ValueError: 当配置无效且处于启动阶段时抛出。
            ConnectionFailure: 当连接失败且处于启动阶段时抛出。
            ServerSelectionTimeoutError: 当连接超时且处于启动阶段时抛出。
            OperationFailure: 当认证或操作失败且处于启动阶段时抛出。
        """
        # 获取配置重载锁，确保同一时刻只有一个重载流程在执行
        async with self._reload_lock:
            # 从配置中心加载最新的 MongoDB 配置
            new_config = self._load_config()

            if not new_config:
                # 配置加载失败或配置无效（例如必需字段缺失、格式错误等）
                error_msg = "MongoDB 配置无效或缺失，无法加载。"
                if not self._started:
                    # 启动阶段：配置无效是致命错误，必须立即抛出异常，阻止系统启动
                    raise ValueError(error_msg)
                else:
                    # 运行时：配置无效时记录错误但不抛异常，保持当前连接继续运行
                    logger.error(error_msg)
                    return

            # 刷新健康检查间隔（根据最新配置中的 mongo_health_interval 参数动态调整）
            self._refresh_health_interval()

            # 比对新旧配置，若完全一致则跳过后续的连接重建流程
            if self._config and self._config.model_dump() == new_config.model_dump():
                if self._client:
                    # 配置未变更且客户端已存在，跳过重载
                    logger.debug("Mongo 配置未变更，跳过重新加载。")
                    return

            try:
                # 配置有变化，创建并测试新的 MongoDB 客户端
                new_client = await self._init_and_test_client(new_config)

                # 先保存旧客户端引用，稍后再关闭
                old_client = self._client

                # 原子性地替换客户端和配置
                self._client = new_client
                self._config = new_config

                if old_client:
                    # 关闭旧客户端，释放连接池资源
                    old_client.close()

                logger.success("MongoManager 重新加载成功")

            except Exception:
                if not self._started:
                    # 启动阶段：任何异常都应快速失败，阻止系统启动
                    raise

    def _refresh_health_interval(self) -> None:
        """刷新健康检查间隔

        从配置中心读取 mongo_health_interval 参数，并动态调整健康检查观察者的检查频率
        该方法允许运维人员在不重启服务的情况下调整健康检查间隔，以适应不同环境的网络状况

        设计说明：
        - 默认间隔为 60 秒（min_interval），最大间隔为 120 秒（max_interval）
        - 配置路径：system.config.timeouts.mongo_health_interval
        - 若配置读取失败或格式错误，会回退到默认值并记录警告，不会影响主流程

        Note:
            该方法在每次 reload() 时都会被调用，确保健康检查间隔与最新配置保持同步
        """
        try:
            # 从配置管理器中获取默认命名空间（通常为 Apollo 的 namespace）
            default_ns = self._settings.local.apollo_namespace
            section = self._settings.get_section(default_ns)

            # 默认健康检查间隔为 60 秒
            interval = 60.0

            if section:
                # 尝试从 system.config 字段中读取配置
                sys_conf = section.values.get("system.config")

                # 若 system.config 是 JSON 字符串，先反序列化
                if isinstance(sys_conf, str):
                    try:
                        sys_conf = json.loads(sys_conf)
                    except Exception:
                        # JSON 解析失败，忽略并使用默认值
                        pass

                # 从配置对象中提取 mongo_health_interval 参数
                if isinstance(sys_conf, dict):
                    timeouts = sys_conf.get("timeouts", {})
                    interval = float(timeouts.get("mongo_health_interval", 60.0))

            # 更新健康检查观察者的间隔时间
            self._health_watcher.update_intervals(interval, interval * 2)
        except Exception as e:
            logger.warning(f"刷新 Mongo 健康检查间隔失败: {e}")

    async def _init_and_test_client(self, config: MongoConfig) -> AsyncIOMotorClient:
        """初始化并测试新的 MongoDB 客户端

        该方法会根据提供的配置创建 Motor 客户端，并执行以下验证步骤：
        1. 建立连接并执行 ping 命令，验证连接可用性
        2. 发现并注册 Beanie 文档模型（ODM 初始化）

        设计考虑：
        - 使用 Motor 的连接池机制（minPoolSize 和 maxPoolSize）优化并发性能
        - 设置 serverSelectionTimeoutMS 以控制连接超时时间，避免长时间阻塞
        - uuidRepresentation="standard" 确保 UUID 类型的字段在存储和读取时使用标准格式
        - 在返回客户端前先测试连接和模型注册，确保返回的客户端立即可用

        Args:
            config (MongoConfig): MongoDB 配置对象，包含连接 URI、数据库名、连接池参数等

        Returns:
            AsyncIOMotorClient: 已初始化并验证的 MongoDB 客户端实例

        Raises:
            ConnectionFailure: 当无法连接到 MongoDB 服务器时抛出
            ServerSelectionTimeoutError: 当连接超时时抛出
            OperationFailure: 当认证失败或数据库操作失败时抛出
            Exception: 其他未预期的错误（例如模型注册失败）
        """
        # 脱敏处理 URI，避免在日志中泄露用户名密码
        masked_uri = config.uri.split("@")[-1] if "@" in config.uri else "localhost"
        logger.bind(uri=masked_uri, db=config.database).info("正在应用新的 Mongo 配置")

        try:
            # 创建 Motor 异步客户端实例
            new_client = AsyncIOMotorClient(
                config.uri,
                minPoolSize=config.min_pool_size,
                maxPoolSize=config.max_pool_size,
                serverSelectionTimeoutMS=config.timeout_ms,
                uuidRepresentation="standard"
            )

            # 获取目标数据库实例
            db = new_client.get_database(config.database)

            # 执行 ping 命令，验证连接是否成功建立
            await db.command("ping")

            # 创建模型注册器，自动发现指定包下的所有 Beanie 文档模型
            new_registry = ModelRegistry()
            models = await new_registry.discover_models(config.models_package)

            if not models:
                # 若未发现任何模型，记录警告
                logger.warning(f"在包 '{config.models_package}' 中未找到任何 MongoDB 模型")

            # 初始化 Beanie ODM
            await init_beanie(database=db, document_models=models)

            # 返回已完成初始化和测试的客户端实例
            return new_client

        except (ConnectionFailure, ServerSelectionTimeoutError, OperationFailure) as e:
            logger.error(f"连接/认证 MongoDB 失败: {e}")
            raise
        except Exception as e:
            logger.error(f"重新加载 MongoDB 时发生未预期的错误: {e}")
            raise

    def _load_config(self) -> Optional[MongoConfig]:
        """从配置中心加载 MongoDB 配置

        该方法会从 SettingsManager 中读取配置数据，并将其反序列化为 MongoConfig 对象
        配置路径：system.config.mongodb（嵌套在 system.config 字段中）

        设计说明：
        - 支持配置字段为 JSON 字符串或字典类型，兼容不同配置中心的存储格式
        - 使用 Pydantic 的 ValidationError 捕获配置校验失败，确保返回的配置对象类型安全
        - 若配置缺失或校验失败，返回 None 而非抛出异常，由调用方决定如何处理

        Returns:
            Optional[MongoConfig]: 成功加载时返回配置对象，失败时返回 None

        Note:
            该方法不会抛出异常，所有错误都会被记录并返回 None
        """
        try:
            # 从配置管理器中获取默认命名空间（通常为 Apollo 的 namespace）
            default_ns = self._settings.local.apollo_namespace
            section = self._settings.get_section(default_ns)

            # 初始化空的 MongoDB 配置数据字典
            mongo_conf_data = {}

            if section:
                # 尝试从 system.config 字段中提取 MongoDB 配置
                sys_conf = section.values.get("system.config")
                if sys_conf:
                    # 若 system.config 是 JSON 字符串，先反序列化
                    if isinstance(sys_conf, str):
                        try:
                            sys_conf = json.loads(sys_conf)
                        except json.JSONDecodeError:
                            # JSON 解析失败，忽略并返回空配置
                            pass

                    # 若 system.config 是字典，提取 mongodb 子字段
                    if isinstance(sys_conf, dict):
                        mongo_conf_data = sys_conf.get("mongodb", {})

            # 使用 Pydantic 模型进行配置校验和反序列化
            return MongoConfig(**mongo_conf_data)

        except ValidationError as e:
            logger.error(f"MongoDB 配置校验失败: {e}")
            return None
        except Exception as e:
            logger.error(f"加载 MongoDB 配置时发生未知错误: {e}")
            return None

    async def health_check(self) -> ResourceHealth:
        """执行一次健康检查

        该方法会尝试向 MongoDB 发送 ping 命令，并根据结果返回资源健康状态
        健康状态包括：INIT（未初始化）、HEALTHY（健康）、UNHEALTHY（不健康）

        设计说明：
        - 若客户端尚未初始化，返回 INIT 状态
        - 若 ping 成功，返回 HEALTHY 状态并附带数据库名称
        - 若 ping 失败，返回 UNHEALTHY 状态并附带错误信息
        - 该方法可被外部健康检查端点（如 /health API）调用，用于服务发现和负载均衡

        Returns:
            ResourceHealth: 包含健康状态、资源类型、名称和详细信息的对象

        Note:
            该方法不会抛出异常，所有错误都会被捕获并反映在健康状态中
        """
        # 获取当前活跃的客户端实例
        client = self._client

        if not client:
            # 客户端尚未初始化，返回 INIT 状态
            return ResourceHealth(kind=self.kind, name=self.name, status=HealthStatus.INIT)

        try:
            # 获取目标数据库名称
            db_name = self.get_target_database_name()

            # 向数据库发送 ping 命令，验证连接健康状态
            await client.get_database(db_name).command("ping")

            # 返回健康状态，附带数据库名称供调试
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.HEALTHY,
                details={"database": db_name}
            )
        except Exception as e:
            # 返回不健康状态，并记录错误信息
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_error=str(e)
            )

    @property
    def client(self) -> AsyncIOMotorClient:
        """获取当前活跃的 MongoDB 客户端实例

        该属性提供对内部客户端的访问，供业务代码使用。若客户端尚未初始化，会抛出 RuntimeError

        设计说明：
        - 使用 @property 装饰器，提供类似属性的访问方式（manager.client）
        - 抛出 RuntimeError 可强制调用方处理未初始化情况，避免静默失败

        Returns:
            AsyncIOMotorClient: 当前活跃的 MongoDB 客户端实例

        Raises:
            RuntimeError: 当客户端尚未初始化时抛出
        """
        if not self._client:
            raise RuntimeError("MongoManager 尚未启动或配置无效。")
        return self._client

    def get_active_client(self) -> Optional[AsyncIOMotorClient]:
        """获取当前活跃的 MongoDB 客户端实例（允许返回 None）

        该方法与 `client` 属性的区别在于，它不会抛出异常，而是直接返回 None
        适用于健康检查等场景，需要容错处理未初始化情况

        Returns:
            Optional[AsyncIOMotorClient]: 当前活跃的客户端实例，若未初始化则返回 None

        Note:
            该方法主要供内部健康检查使用，外部业务代码应优先使用 `client` 属性
        """
        return self._client

    def get_target_database_name(self) -> str:
        """获取目标数据库名称

        从当前配置中提取数据库名称，若配置未初始化则返回默认值 "admin"

        设计说明：
        - 返回默认值 "admin" 是为了在配置缺失时仍能执行基本的健康检查（admin 数据库通常存在）
        - 业务代码应避免直接依赖该方法，应通过配置明确指定数据库名称

        Returns:
            str: 目标数据库名称，默认为 "admin"

        Note:
            该方法主要供健康检查使用，不建议用于业务逻辑
        """
        return self._config.database if self._config else "admin"
