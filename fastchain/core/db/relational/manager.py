from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional, override

from loguru import logger
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncEngine
)
from sqlalchemy.pool import NullPool

from .config import RelationalDBConfig
from .models import Base
from ....core.config import SettingsManager
from ....core.config.constants import ResourceName, ResourceKind
from ....core.logging.logger import intercept_loggers
from ....core.resources.base import Resource, ResourceHealth, HealthStatus
from ....core.resources.events import Event, EventBus, EventConstants
from ....core.resources.limits import ResourceLimiter
from ....core.resources.watcher import BackgroundWatcher

# SQLAlchemy 日志记录器名称常量
# 这些日志记录器会产生大量调试信息，需要通过 loguru 进行拦截和过滤
# 引擎层日志（SQL 语句、连接事件等）
LOGGER_SA_ENGINE = "sqlalchemy.engine"
# 连接池日志（连接获取、释放、回收等）
LOGGER_SA_POOL = "sqlalchemy.pool"

# SQL 健康检查查询语句，SELECT 1 是最轻量级的查询，几乎所有关系型数据库都支持，用于验证连接可用性
SQL_PING_QUERY = "SELECT 1"


def _custom_json_serializer(obj: Any, **kwargs: Any) -> str:
    """自定义 JSON 序列化器，用于 SQLAlchemy 的 JSON 字段

    该序列化器会强制禁用 ASCII 转义，确保中文等非 ASCII 字符以原始形式存储
    同时使用 default=str 作为回退策略，处理无法直接序列化的对象（如日期、UUID 等）

    设计说明：
    - ensure_ascii=False 确保中文等 Unicode 字符不被转义为 \\uXXXX 形式，提升可读性
    - default=str 作为通用回退，可处理大部分自定义类型（如 datetime、Decimal、UUID 等）
    - 该序列化器会被 SQLAlchemy 引擎用于处理 JSON/JSONB 列的序列化

    Args:
        obj (Any): 需要序列化的 Python 对象
        **kwargs (Any): 传递给 json.dumps 的额外参数

    Returns:
        str: JSON 格式的字符串

    Note:
        若对象包含无法序列化的复杂类型（如自定义类实例），会回退为其字符串表示
    """
    # 强制设置 ensure_ascii=False，避免中文被转义
    kwargs["ensure_ascii"] = False

    # 使用 default=str 作为回退策略，将无法序列化的对象转为字符串
    # 这对于日期、时间、UUID、Decimal 等类型特别有用
    return json.dumps(obj, default=str, **kwargs)


@dataclass(frozen=True)
class DBState:
    """数据库状态容器（不可变）

    该类使用 dataclass 封装数据库引擎、会话工厂和配置对象，作为 RelationalDBManager 的内部状态
    使用 frozen=True 确保状态不可变，防止意外修改导致的并发问题或状态不一致

    设计考虑：
    - 不可变设计（frozen=True）确保线程安全，多个协程可以安全地读取同一个状态对象
    - 将引擎、会话工厂和配置封装在一起，确保它们始终保持一致（原子性替换）
    - 在配置重载时，整个 DBState 对象会被原子性地替换，避免部分更新导致的不一致状态

    Attributes:
        engine (AsyncEngine): SQLAlchemy 异步引擎实例，负责管理数据库连接池
        session_maker (async_sessionmaker): 异步会话工厂，用于创建数据库会话
        config (RelationalDBConfig): 当前生效的数据库配置对象

    Note:
        该类的实例在创建后不能被修改，任何配置变更都需要创建新的 DBState 实例
    """
    engine: AsyncEngine
    session_maker: async_sessionmaker
    config: RelationalDBConfig


class DBHealthWatcher(BackgroundWatcher):
    """关系型数据库主动健康检查观察者

    该类继承自 BackgroundWatcher，专门负责定期对关系型数据库连接进行主动健康检查
    通过定期执行轻量级 SQL 查询（SELECT 1），可以及时发现连接异常、网络抖动或数据库宕机

    增强特性：
    - 集成 ResourceLimiter：支持可选的资源限流
    - 集成 EventBus：支持向上传递事件总线，激活基类的可观测性能力

    设计考虑：
    - 使用 SELECT 1 而非复杂查询，最小化对数据库的性能影响
    - 通过可配置的检查间隔（min_interval 和 max_interval），支持动态调整以适应不同环境
    - 采用主动探测而非被动等待首次失败，可更早发现潜在问题，减少服务中断时间
    - 通过 BackgroundWatcher 的异步调度机制，避免阻塞主业务逻辑

    Attributes:
        self._manager (RelationalDBManager): 所关联的数据库管理器实例，用于获取活跃引擎

    Methods:
        tick: 执行一次健康检查的核心逻辑
    """

    def __init__(
        self,
        manager: RelationalDBManager,
        event_bus: EventBus | None = None,
        limiter: ResourceLimiter | None = None
    ):
        """初始化数据库健康检查观察者

        Args:
            manager (RelationalDBManager): 所关联的 RelationalDBManager 实例
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

        该方法会获取当前活跃的数据库引擎，并执行轻量级 SQL 查询（SELECT 1）验证连接可用性
        若查询失败，会抛出异常（由 BackgroundWatcher 的异常处理机制捕获并记录）

        设计说明：
        - 使用 SELECT 1 是业界标准的健康检查方式，兼容所有主流关系型数据库
        - 若引擎尚未初始化（engine 为 None），则跳过本次检查，避免在启动阶段产生噪音日志
        - 使用 engine.connect() 而非 session，避免引入 ORM 层的额外开销
        - 异常会被 BackgroundWatcher 基类捕获，并根据配置决定是否触发告警或重启逻辑

        Raises:
            sqlalchemy.exc.SQLAlchemyError: 当数据库连接异常或查询失败时抛出
        """
        # 获取当前活跃的数据库引擎实例
        engine = self._manager.get_active_engine()
        if not engine:
            # 引擎尚未初始化，跳过本次健康检查
            return

        # 创建一个临时连接并执行健康检查查询
        async with engine.connect() as conn:
            # 执行查询，验证数据库连接健康状态
            await conn.execute(text(SQL_PING_QUERY))


class RelationalDBManager(Resource):
    """关系型数据库资源管理器（基于 SQLAlchemy 异步引擎）

    该类负责管理关系型数据库连接的完整生命周期，包括引擎初始化、配置热重载、健康检查和优雅关闭
    作为系统核心资源之一，RelationalDBManager 实现了与配置中心（如 Apollo）的集成，支持动态更新连接参数，并通过事件总线订阅配置变更事件

    核心职责：
    - 从配置中心加载数据库连接参数（URL、连接池大小、回收时间等）
    - 初始化并维护 SQLAlchemy 异步引擎实例，支持连接池复用和连接预检
    - 实现配置热重载：当检测到配置变更时，自动重新创建引擎（仅当参数真正变化时）
    - 提供会话工厂（session_maker），供业务代码创建数据库会话
    - 提供主动健康检查机制，定期验证连接可用性
    - 在启动和运行时采用不同的错误处理策略（启动时快速失败，运行时容错降级）
    - 针对不同数据库类型（SQLite vs 传统 RDBMS）采用差异化的连接池策略

    设计考虑：
    - 使用 asyncio.Lock 保护配置重载过程，防止并发重载导致的资源泄漏或状态不一致
    - 通过比对配置哈希值（model_dump）避免无效重载，减少不必要的引擎重建开销
    - 健康检查间隔可动态调整，适应不同环境的网络延迟和可靠性要求
    - 在关闭旧引擎前先创建并测试新引擎，实现平滑切换，最小化服务中断窗口
    - 使用 DBState 不可变容器确保引擎、会话工厂和配置的原子性替换
    - 拦截 SQLAlchemy 的标准库日志记录器，将日志统一路由到 loguru 以保持一致性
    - 支持自动建表（仅限开发环境），生产环境应使用数据库迁移工具（如 Alembic）

    Attributes:
        self._settings (SettingsManager): 配置管理器，用于读取和订阅配置变更
        self._event_bus (EventBus): 事件总线，用于订阅配置更新事件
        self._state (DBState | None): 当前活跃的数据库状态容器（引擎、会话工厂、配置）
        self._reload_lock (asyncio.Lock): 配置重载锁，防止并发重载导致竞态条件
        self._started (bool): 标记管理器是否已启动，用于区分启动阶段和运行阶段的错误处理策略
        self._health_watcher (DBHealthWatcher): 健康检查观察者实例

    Methods:
        start: 启动数据库管理器，初始化引擎并订阅配置事件
        stop: 停止数据库管理器，关闭引擎和健康检查
        reload: 重新加载配置并重建引擎（若配置有变化）
        health_check: 执行一次健康检查并返回健康状态
        get_active_engine: 获取当前活跃的引擎实例（可能为 None）
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus):
        """初始化关系型数据库资源管理器

        Args:
            settings (SettingsManager): 配置管理器实例，用于读取配置和订阅变更事件
            event_bus (EventBus): 事件总线实例，用于订阅和发布系统事件
        """
        # 调用父类构造函数，设置资源名称、类型和启动优先级
        super().__init__(
            name=ResourceName.SQL_DB,
            kind=ResourceKind.DATABASE,
            startup_priority=10
        )
        self._settings = settings
        self._event_bus = event_bus

        # 初始化内部状态
        self._state: DBState | None = None

        # 创建异步锁，用于保护 reload() 方法的并发执行
        self._reload_lock = asyncio.Lock()

        # 标记管理器是否已成功启动
        self._started = False

        # 创建健康检查观察者实例
        self._health_watcher = DBHealthWatcher(self, event_bus=self._event_bus)

    async def start(self) -> None:
        """启动关系型数据库管理器

        该方法会执行以下操作：
        1. 订阅配置中心的配置更新事件
        2. 加载初始配置并创建数据库引擎
        3. 启动后台健康检查观察者
        4. 自动创建数据库表（仅限开发环境，生产环境应禁用）

        设计说明：
        - 若已启动则跳过，避免重复初始化
        - 在启动阶段，若配置加载或引擎创建失败会立即抛出异常（快速失败），确保系统不会带病运行
        - 启动成功后，_started 标志会被设为 True，后续的配置重载会采用容错策略
        - 自动建表功能仅在开发环境使用，生产环境应通过数据库迁移工具（如 Alembic）管理表结构

        Raises:
            ValueError: 当初始配置无效或缺失时抛出
            sqlalchemy.exc.SQLAlchemyError: 当无法连接到数据库或创建引擎失败时抛出

        Warning:
            自动建表功能（create_all）仅适用于开发环境，生产环境应禁用此功能，使用数据库迁移工具管理表结构变更，避免数据丢失或结构不一致
        """
        if self._started:
            # 已启动，跳过重复初始化
            return

        logger.info("正在启动关系型数据库管理器...")

        # 订阅配置中心的配置更新事件
        self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        try:
            # 加载初始配置并创建引擎
            await self.reload()
        except Exception:
            logger.critical("关系型数据库管理器启动失败")
            raise

        # 启动后台健康检查观察者，观察者会按配置的时间间隔定期执行健康检查，监控连接可用性
        await self._health_watcher.start()

        self._started = True  # 标记为已启动

        # 自动建表逻辑（仅适用于开发环境）
        # 生产环境应通过数据库迁移工具（如 Alembic）管理表结构
        if self._state:
            try:
                # 使用 begin() 开启事务，确保建表操作的原子性
                async with self._state.engine.begin() as conn:
                    # run_sync 将同步的 create_all 方法包装为异步调用
                    # Base.metadata.create_all 会创建所有继承自 Base 的模型对应的表
                    # 若表已存在则跳过，不会报错
                    await conn.run_sync(Base.metadata.create_all)
            except Exception as e:
                logger.warning(f"自动创建表失败: {e}")

    async def stop(self) -> None:
        """停止关系型数据库管理器

        该方法会执行以下操作：
        1. 停止后台健康检查观察者
        2. 关闭数据库引擎并释放连接池资源
        3. 清理内部状态

        设计说明：
        - 确保优雅关闭，避免连接泄漏或资源未释放
        - 停止顺序：先停止健康检查（避免在关闭过程中触发误报），再关闭引擎
        - 使用 engine.dispose() 而非 close()，确保连接池中的所有连接都被正确关闭
        """
        logger.info("正在停止关系型数据库管理器...")

        # 停止后台健康检查观察者
        await self._health_watcher.stop()

        # 标记为已停止
        self._started = False

        if self._state:
            # 关闭数据库引擎并释放连接池资源，dispose() 会关闭连接池中的所有连接，比 close() 更彻底，这是 SQLAlchemy 推荐的优雅关闭方式
            await self._state.engine.dispose()
            # 清空状态引用
            self._state = None

    async def _on_config_updated(self, _event: Event) -> None:
        """配置更新事件回调

        当配置中心推送新配置时，该方法会被事件总线调用。方法会检查数据库相关配置是否变更，若有变化则触发热重载

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

        logger.debug("检测到配置更新，检查是否需要重新加载数据库...")
        try:
            # 尝试重新加载配置并重建引擎（若配置确有变化）
            await self.reload()
        except Exception:
            logger.exception("数据库管理器热重载失败")

    async def reload(self) -> None:
        """重新加载配置并重建数据库引擎（若配置有变化）

        该方法是配置热重载的核心逻辑，执行以下步骤：
        1. 加锁，防止并发重载
        2. 从配置中心读取最新的数据库配置
        3. 刷新健康检查间隔（根据最新配置动态调整）
        4. 比对新旧配置，若完全一致则跳过重建
        5. 若配置有变化，创建新引擎并测试连接
        6. 连接成功后，替换旧引擎并关闭旧连接池

        设计考虑：
        - 使用 asyncio.Lock 确保同一时刻只有一个 reload 流程在执行，避免资源泄漏
        - 通过配置对象的 model_dump() 比对哈希值，避免配置未变更时的无效重载
        - 采用"先建后删"策略：先创建并验证新引擎，成功后再关闭旧引擎，最小化服务中断时间
        - 在启动阶段（_started=False），若配置无效或引擎创建失败会抛出异常（快速失败）
        - 在运行时（_started=True），若配置无效会记录错误但不抛异常，保持当前引擎继续运行（容错）
        - 使用 DBState 不可变容器确保引擎、会话工厂和配置的原子性替换

        Raises:
            ValueError: 当配置无效且处于启动阶段时抛出
            sqlalchemy.exc.SQLAlchemyError: 当引擎创建或连接测试失败且处于启动阶段时抛出
        """
        # 获取配置重载锁，确保同一时刻只有一个重载流程在执行
        async with self._reload_lock:
            # 从配置中心加载最新的数据库配置
            new_config = self._load_config()

            if not new_config:
                # 配置加载失败或配置无效（例如必需字段缺失、格式错误等）
                error_msg = "数据库配置无效或缺失。"
                if not self._started:
                    # 启动阶段：配置无效是致命错误，必须立即抛出异常，阻止系统启动
                    raise ValueError(error_msg)
                else:
                    # 运行时：配置无效时记录错误但不抛异常，保持当前引擎继续运行
                    logger.error(error_msg)
                    return

            # 刷新健康检查间隔（根据最新配置中的 db_health_interval 参数动态调整）
            self._refresh_health_interval()

            # 比对新旧配置，若完全一致则跳过后续的引擎重建流程
            if self._state and self._state.config.model_dump() == new_config.model_dump():
                logger.debug("数据库配置未变更，跳过重新加载。")
                return

            # 生成安全的 URL 字符串用于日志记录
            try:
                safe_url_str = make_url(new_config.url).render_as_string(hide_password=True)
            except Exception:
                # 若 URL 格式无效，使用占位符
                safe_url_str = "[Invalid URL]"

            logger.info(f"正在应用新的数据库配置: {safe_url_str}")

            # 初始化新引擎变量，用于异常处理时的资源清理
            new_engine = None
            try:
                # 创建新的数据库引擎
                new_engine = self._create_engine(new_config)

                # 测试新引擎的连接可用性，使用 connect() 而非 session 以避免 ORM 层的额外开销
                async with new_engine.connect() as conn:
                    # 执行查询，验证连接是否正常
                    await conn.execute(text(SQL_PING_QUERY))

                # 创建新地会话工厂
                new_maker = async_sessionmaker(
                    bind=new_engine,
                    expire_on_commit=False,
                    autoflush=False
                )

                # 先保存旧状态引用，稍后再清理
                old_state = self._state

                # 原子性地替换状态容器，使用不可变的 DBState 确保引擎、会话工厂和配置始终保持一致
                self._state = DBState(engine=new_engine, session_maker=new_maker, config=new_config)

                if old_state:
                    logger.info("正在释放旧的数据库引擎...")
                    await old_state.engine.dispose()

                logger.success("关系型数据库重新加载成功。")

            except Exception as e:
                logger.error(f"重新加载数据库引擎失败: {e}")

                # 若新引擎已创建但测试失败，需要手动清理资源
                if new_engine:
                    await new_engine.dispose()

                if not self._started:
                    # 启动阶段：任何异常都应快速失败，阻止系统启动
                    raise

    def _refresh_health_interval(self) -> None:
        """刷新健康检查间隔

        从配置中心读取 db_health_interval 参数，并动态调整健康检查观察者的检查频率
        该方法允许运维人员在不重启服务的情况下调整健康检查间隔，以适应不同环境的网络状况

        设计说明：
        - 默认间隔为 60 秒（min_interval），最大间隔为 120 秒（max_interval）
        - 配置路径：system.config.timeouts.db_health_interval
        - 若配置读取失败或格式错误，会回退到默认值并记录警告，不会影响主流程

        Note:
            该方法在每次 reload() 时都会被调用，确保健康检查间隔与最新配置保持同步
        """
        try:
            # 从配置管理器中获取默认命名空间
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

                # 从配置对象中提取 db_health_interval 参数
                if isinstance(sys_conf, dict):
                    timeouts = sys_conf.get("timeouts", {})
                    interval = float(timeouts.get("db_health_interval", 60.0))

            # 更新健康检查观察者的间隔时间
            self._health_watcher.update_intervals(interval, interval * 2)
        except Exception as e:
            logger.warning(f"刷新 DB 健康检查间隔失败: {e}")

    def _load_config(self) -> Optional[RelationalDBConfig]:
        """从配置中心加载数据库配置

        该方法会从 SettingsManager 中读取配置数据，并将其反序列化为 RelationalDBConfig 对象
        配置路径：system.config.database（嵌套在 system.config 字段中）

        设计说明：
        - 支持配置字段为 JSON 字符串或字典类型，兼容不同配置中心的存储格式
        - 使用 Pydantic 的 ValidationError 捕获配置校验失败，确保返回的配置对象类型安全
        - 若配置缺失或校验失败，返回 None 而非抛出异常，由调用方决定如何处理

        Returns:
            Optional[RelationalDBConfig]: 成功加载时返回配置对象，失败时返回 None

        Note:
            该方法不会抛出异常，所有错误都会被记录并返回 None
        """
        try:
            # 从配置管理器中获取默认命名空间
            default_ns = self._settings.local.apollo_namespace
            section = self._settings.get_section(default_ns)

            # 初始化空的数据库配置数据字典
            db_config_data = {}

            if section:
                # 尝试从 system.config 字段中提取数据库配置
                sys_conf = section.values.get("system.config")
                if sys_conf:
                    # 若 system.config 是 JSON 字符串，先反序列化
                    if isinstance(sys_conf, str):
                        try:
                            sys_conf = json.loads(sys_conf)
                        except json.JSONDecodeError:
                            # JSON 解析失败，忽略并返回空配置
                            pass

                    # 若 system.config 是字典，提取 database 子字段
                    if isinstance(sys_conf, dict):
                        db_config_data = sys_conf.get("database", {})

            # 使用 Pydantic 模型进行配置校验和反序列化
            return RelationalDBConfig(**db_config_data)

        except ValidationError as e:
            logger.error(f"数据库配置无效: {e}")
            return None
        except Exception as e:
            logger.error(f"加载数据库配置时出错: {e}")
            return None

    def _create_engine(self, config: RelationalDBConfig) -> AsyncEngine:
        """创建 SQLAlchemy 异步引擎

        该方法会根据提供的配置创建异步引擎，并设置连接池、日志级别等参数
        针对不同数据库类型（SQLite vs 传统 RDBMS）采用差异化的连接池策略

        设计考虑：
        - 对于 SQLite，使用 NullPool（无连接池）以避免文件锁冲突和并发问题
        - 对于传统 RDBMS（MySQL、PostgreSQL 等），使用默认连接池并配置 pool_pre_ping 确保连接可靠性
        - 拦截 SQLAlchemy 的标准库日志记录器，将日志统一路由到 loguru 以保持一致性
        - 使用自定义 JSON 序列化器处理 JSON/JSONB 字段，确保中文等非 ASCII 字符正确存储

        Args:
            config (RelationalDBConfig): 数据库配置对象，包含连接 URL、连接池参数等

        Returns:
            AsyncEngine: 已配置的 SQLAlchemy 异步引擎实例

        Note:
            该方法创建的引擎尚未测试连接可用性，调用方应在返回后执行连接测试
        """
        # 解析数据库 URL，用于后续判断数据库类型
        safe_url = make_url(config.url)

        # 初始化引擎参数字典
        engine_kwargs = {
            # 禁用 SQL 语句回显（通过日志级别控制）
            "echo": False,
            # 使用自定义 JSON 序列化器
            "json_serializer": _custom_json_serializer
        }

        # 拦截 SQLAlchemy 的标准库日志记录器，将日志路由到 loguru，确保所有日志格式一致，便于统一管理和分析
        loggers_to_intercept = [LOGGER_SA_ENGINE, LOGGER_SA_POOL]
        intercept_loggers(loggers_to_intercept)

        # 根据配置动态设置日志级别
        if config.echo:
            # 开启 SQL 语句回显，用于开发调试
            logging.getLogger(LOGGER_SA_ENGINE).setLevel(logging.INFO)
            logging.getLogger(LOGGER_SA_POOL).setLevel(logging.INFO)
        else:
            # 生产环境应禁用回显，仅记录警告和错误
            logging.getLogger(LOGGER_SA_ENGINE).setLevel(logging.WARNING)
            logging.getLogger(LOGGER_SA_POOL).setLevel(logging.WARNING)

        # 针对不同数据库类型采用差异化的连接池策略
        if safe_url.drivername.startswith("sqlite"):
            # SQLite 数据库：使用 NullPool（无连接池）
            engine_kwargs["poolclass"] = NullPool
        else:
            # 传统 RDBMS（MySQL、PostgreSQL、Oracle 等）：使用默认连接池
            engine_kwargs["pool_size"] = config.pool_size

            # max_overflow：允许临时创建的额外连接数
            engine_kwargs["max_overflow"] = config.max_overflow

            # pool_pre_ping：在每次从池中获取连接前执行 ping 测试
            engine_kwargs["pool_pre_ping"] = True

            # pool_recycle：连接回收时间（秒）
            engine_kwargs["pool_recycle"] = config.pool_recycle

        # 创建并返回异步引擎
        return create_async_engine(config.url, **engine_kwargs)

    async def health_check(self) -> ResourceHealth:
        """执行一次健康检查

        该方法会尝试创建数据库连接并执行轻量级查询（SELECT 1），根据结果返回资源健康状态
        健康状态包括：INIT（未初始化）、HEALTHY（健康）、UNHEALTHY（不健康）

        设计说明：
        - 若状态尚未初始化，返回 INIT 状态
        - 若查询成功，返回 HEALTHY 状态
        - 若查询失败，返回 UNHEALTHY 状态并附带错误信息
        - 该方法可被外部健康检查端点（如 /health API）调用，用于服务发现和负载均衡

        Returns:
            ResourceHealth: 包含健康状态、资源类型、名称和详细信息的对象

        Note:
            该方法不会抛出异常，所有错误都会被捕获并反映在健康状态中
        """
        if not self._state:
            # 状态尚未初始化，返回 INIT 状态
            return ResourceHealth(kind=self.kind, name=self.name, status=HealthStatus.INIT)

        try:
            # 创建一个临时连接并执行健康检查查询
            async with self._state.engine.connect() as conn:
                # 执行查询，验证数据库连接健康状态
                await conn.execute(text(SQL_PING_QUERY))

            # 返回健康状态
            return ResourceHealth(kind=self.kind, name=self.name, status=HealthStatus.HEALTHY)
        except Exception as e:
            # 返回不健康状态，并记录错误信息
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_error=str(e)
            )

    @property
    def session_maker(self) -> async_sessionmaker:
        """获取异步会话工厂

        该属性提供对会话工厂的访问，供业务代码创建数据库会话。若状态尚未初始化，会抛出 RuntimeError

        设计说明：
        - 使用 @property 装饰器，提供类似属性的访问方式（manager.session_maker）
        - 抛出 RuntimeError 可强制调用方处理未初始化情况，避免静默失败
        - 业务代码应通过 async with session_maker() as session 创建会话

        Returns:
            async_sessionmaker: 异步会话工厂实例

        Raises:
            RuntimeError: 当状态尚未初始化时抛出
        """
        if not self._state:
            raise RuntimeError("关系型数据库管理器未启动。")
        return self._state.session_maker

    def get_active_engine(self) -> Optional[AsyncEngine]:
        """获取当前活跃的数据库引擎实例（允许返回 None）

        该方法与 `session_maker` 属性的区别在于，它不会抛出异常，而是直接返回 None。适用于健康检查等场景，需要容错处理未初始化情况

        Returns:
            Optional[AsyncEngine]: 当前活跃的引擎实例，若未初始化则返回 None

        Note:
            该方法主要供内部健康检查使用，外部业务代码应优先使用 `session_maker` 属性
        """
        if self._state:
            return self._state.engine
        return None
