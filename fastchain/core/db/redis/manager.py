from __future__ import annotations

import asyncio
import json
import time
from typing import Set, Optional, override

import redis.asyncio as redis
from loguru import logger
from pydantic import ValidationError
from redis.exceptions import RedisError, AuthenticationError

from .config import RedisConfig
from ....core.config import SettingsManager
from ....core.config.constants import ResourceName, AppConstants, ResourceKind
from ....core.resources.base import Resource, ResourceHealth, HealthStatus
from ....core.resources.events import Event, EventBus, EventConstants
from ....core.resources.limits import ResourceLimiter
from ....core.resources.watcher import BackgroundWatcher


class RedisHealthWatcher(BackgroundWatcher):
    """Redis 主动健康检查观察者

    该类继承自 BackgroundWatcher，实现了基于定时轮询的主动健康检查
    与被动健康检查（health_check() 方法）不同，主动健康检查会持续在后台执行 PING 命令，及早发现连接问题

    增强特性：
    - 集成 ResourceLimiter：支持可选的资源限流
    - 集成 EventBus：支持向上传递事件总线，激活基类的可观测性能力

    设计考量：

    1. 为何需要主动健康检查？
       - Redis 连接可能在空闲时被服务端关闭（timeout）
       - 网络故障可能导致连接静默失败（半开连接）
       - 主动 PING 可以及早发现问题，触发重连

    2. 为何使用 BackgroundWatcher？
       - 提供自适应退避重试（避免雪崩效应）
       - 提供抖动机制（避免多实例同时检查）
       - 提供优雅关闭（响应停止信号）
       - 提供超时保护（避免 PING 死锁）

    3. 检查间隔的选择：
       - min_interval=30 秒：正常频率（避免过于频繁）
       - max_interval=60 秒：失败后退避（减少无效请求）
       - 可通过配置文件热重载调整

    Attributes:
        _manager (RedisManager): Redis 资源管理器引用

    Methods:
        tick: 执行一次健康检查（PING 命令）

    Note:
        - 该类是 BackgroundWatcher 的具体实现（Template Method 模式）
        - tick() 方法会被 BackgroundWatcher 定时调用
        - 异常会被 BackgroundWatcher 捕获并触发退避重试
    """

    def __init__(
        self,
        manager: RedisManager,
        event_bus: EventBus | None = None,
        limiter: ResourceLimiter | None = None
    ):
        """初始化 Redis 健康检查观察者

        Args:
            manager (RedisManager): Redis 资源管理器引用，用于获取活跃的客户端。
            event_bus (EventBus | None): 事件总线，用于基类发布内部事件
            limiter (ResourceLimiter | None): 资源限流器，用于控制检查频率（可选）

        Note:
            初始化参数说明：
            - name: 观察者名称，格式为 "{manager.name}-health"（如 "redis-health"）
            - logger: 使用全局 loguru logger（统一日志格式）
            - min_interval: 30 秒（正常检查频率）
            - max_interval: 60 秒（失败后退避频率）

            为何使用 manager 引用而非直接持有 client？
            - client 可能在热重载时被替换（_client 引用变化）
            - 通过 manager.get_active_client() 始终获取最新的 client
            - 避免持有过期的 client 引用（导致检查无效）
        """
        # 调用父类构造函数，初始化 BackgroundWatcher
        # 将 event_bus 和 limiter 传递给基类，确保 BackgroundWatcher 能够利用框架能力
        super().__init__(
            name=f"{manager.name}-health",
            logger=logger,
            min_interval=30.0,
            max_interval=60.0,
            event_bus=event_bus,
            limiter=limiter
        )

        # 保存 manager 引用，用于获取活跃的 Redis 客户端
        self._manager = manager

    @override
    async def tick(self) -> None:
        """执行一次健康检查（PING 命令）

        该方法会被 BackgroundWatcher 定时调用（每 30-60 秒一次）。通过发送 PING 命令验证 Redis 连接是否正常

        Raises:
            RedisError: Redis 连接错误（如连接断开、超时、认证失败）
            asyncio.TimeoutError: PING 命令超时（由 BackgroundWatcher 触发）
        """
        # 获取当前活跃的 Redis 客户端
        client = self._manager.get_active_client()

        # 若客户端为空，直接返回（不抛出异常）
        if not client:
            return

        # 执行 PING 命令，验证 Redis 连接是否正常
        await client.ping()


class RedisManager(Resource):
    """Redis 资源管理器，基于 redis-py 的异步客户端实现

    该类是 Redis 资源的统一管理入口，负责：
    - 连接池的创建、管理和销毁
    - 配置的加载、验证和热重载
    - 健康检查（主动和被动）
    - 优雅关闭和资源回收

    核心特性：

    1. 热重载（Hot Reload）：
       - 监听 Apollo 配置更新事件
       - 动态切换 Redis 连接（无需重启服务）
       - 平滑过渡（旧连接延迟关闭，新连接立即生效）

    2. 健康检查（Health Check）：
       - 主动检查：后台定时 PING（RedisHealthWatcher）
       - 被动检查：按需调用 health_check()（Kubernetes Probe）
       - 连接池监控：跟踪连接数、延迟等指标

    3. 优雅关闭（Graceful Shutdown）：
       - 停止健康检查任务
       - 取消所有后台任务（如延迟关闭旧连接）
       - 关闭连接池（等待活跃连接完成）

    4. 配置降级（Configuration Degradation）：
       - 启动阶段：配置无效或缺失时抛出异常（快速失败）
       - 运行阶段：配置更新失败时保持旧配置（保持可用性）

    架构设计：

    ```
    RedisManager（资源管理器）
    ├── 生命周期管理
    │   ├── start()           ← 启动（订阅事件 + 加载配置 + 启动健康检查）
    │   ├── stop()            ← 停止（停止健康检查 + 清理任务 + 关闭连接池）
    │   └── reload()          ← 热重载（加载配置 + 切换连接 + 延迟关闭旧连接）
    ├── 配置管理
    │   ├── _load_config()    ← 加载配置（从 SettingsManager）
    │   └── _on_config_updated() ← 事件回调（监听 Apollo 更新）
    ├── 连接管理
    │   ├── _init_and_test_client() ← 创建并测试连接
    │   └── _safe_close_client()    ← 安全关闭旧连接（延迟 5 秒）
    ├── 健康检查
    │   ├── RedisHealthWatcher   ← 主动健康检查（后台定时任务）
    │   └── health_check()       ← 被动健康检查（按需调用）
    └── 客户端访问
        ├── client (property)    ← 获取客户端（运行时检查）
        └── get_active_client()  ← 获取客户端（返回 Optional）
    ```

    热重载流程：

    ```
    Apollo 配置更新
    ↓
    EventBus 触发 "config.updated" 事件
    ↓
    _on_config_updated() 被调用
    ↓
    reload() 执行
    ├── 加载新配置（_load_config）
    ├── 配置验证（RedisConfig）
    ├── 配置对比（避免无效重连）
    ├── 创建新连接（_init_and_test_client）
    ├── 测试新连接（SET + GET）
    ├── 原子切换（self._client = new_client）
    └── 延迟关闭旧连接（5 秒后关闭，避免影响正在进行的请求）
    ```

    配置降级策略：

    启动阶段（_started=False）：
    - 配置无效 → 抛出异常（快速失败，避免服务以错误状态运行）
    - 连接失败 → 抛出异常（快速失败，避免服务无法访问 Redis）

    运行阶段（_started=True）：
    - 配置无效 → 保持旧配置，记录错误日志（保持可用性）
    - 连接失败 → 保持旧连接，记录错误日志（保持可用性）

    Attributes:
        _settings (SettingsManager): 配置管理器（用于加载 Redis 配置）
        _event_bus (EventBus): 事件总线（用于订阅配置更新事件）
        _client (redis.Redis | None): 当前活跃的 Redis 客户端
        _config (RedisConfig | None): 当前生效的 Redis 配置
        _reload_lock (asyncio.Lock): 热重载锁（避免并发重载）
        _started (bool): 启动状态标志
        _background_tasks (Set[asyncio.Task]): 后台任务集合（如延迟关闭旧连接）
        _health_watcher (RedisHealthWatcher): 主动健康检查器

    Methods:
        start: 启动资源管理器
        stop: 停止资源管理器
        reload: 热重载配置和连接
        health_check: 被动健康检查（返回资源健康状态）
        client (property): 获取 Redis 客户端（运行时检查）
        get_active_client: 获取活跃的 Redis 客户端（返回 Optional）

    Note:
        - 该类继承自 Resource（资源抽象基类）
        - 实现了资源生命周期管理（start、stop、health_check）
        - 支持事件驱动（订阅 Apollo 配置更新事件）
        - 线程安全性：使用 asyncio.Lock 保护热重载过程
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus):
        """初始化 Redis 资源管理器

        Args:
            settings (SettingsManager): 配置管理器，用于加载 Redis 配置
            event_bus (EventBus): 事件总线，用于订阅配置更新事件

        Note:
            初始化流程：
            1. 调用父类构造函数（Resource）
            2. 保存依赖注入的对象（settings、event_bus）
            3. 初始化状态变量（client、config、started）
            4. 创建同步原语（reload_lock）
            5. 创建后台任务管理结构（background_tasks）
            6. 创建健康检查器（health_watcher）

            为何在构造函数中创建 health_watcher？
            - health_watcher 是管理器的一部分（组合关系）
            - 生命周期与管理器一致（start/stop 时启动/停止）
            - 需要引用管理器（获取活跃客户端）

            为何不在构造函数中创建 client？
            - client 的创建需要配置（从 SettingsManager 加载）
            - 创建过程可能失败（网络错误、认证失败）
            - 异步操作（await），不能在 __init__ 中执行
            - 应在 start() 中创建（异步上下文）
        """
        # 调用父类构造函数，初始化资源基类
        super().__init__(
            name=ResourceName.REDIS,
            kind=ResourceKind.CACHE,
            startup_priority=10
        )

        # 保存配置管理器引用
        self._settings = settings

        # 保存事件总线引用
        self._event_bus = event_bus

        # Redis 客户端（连接池）
        self._client: redis.Redis | None = None

        # Redis 配置
        self._config: RedisConfig | None = None

        # 热重载锁（asyncio.Lock）
        self._reload_lock = asyncio.Lock()

        # 启动状态标志，False: 未启动（构造后、启动前、停止后），True: 已启动（正常运行）
        self._started = False

        # 后台任务集合（Set[asyncio.Task]）
        self._background_tasks: Set[asyncio.Task] = set()

        # 主动健康检查器
        self._health_watcher = RedisHealthWatcher(self, event_bus=self._event_bus)

    async def start(self) -> None:
        """启动 Redis 资源管理器

        该方法执行以下操作：
        1. 订阅配置更新事件（Apollo）
        2. 加载初始配置并创建连接
        3. 启动主动健康检查

        Note:
            幂等性设计：
            - 若已启动（_started=True），直接返回
            - 避免重复订阅事件（导致回调被多次调用）
            - 避免重复创建连接（导致资源泄漏）

        Raises:
            ValueError: 配置无效或缺失
            RedisError: Redis 连接错误
            AuthenticationError: Redis 认证失败
            Exception: 其他未预期的错误
        """
        # 幂等性检查：若已启动，直接返回
        if self._started:
            return

        # 记录启动日志
        logger.info("正在启动 RedisManager...")

        # 监听 Apollo 配置更新事件，回调函数：self._on_config_updated
        self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        try:
            # 加载初始配置并创建连接
            await self.reload()

        except Exception:
            # 启动阶段的任何异常都应该抛出（快速失败）
            logger.critical("RedisManager 启动失败")
            raise

        # 启动后台健康检查任务
        await self._health_watcher.start()

        # 设置启动标志
        self._started = True

    async def stop(self) -> None:
        """停止 Redis 资源管理器

        该方法执行以下操作：
        1. 停止主动健康检查
        2. 取消所有后台任务
        3. 关闭 Redis 连接池
        """
        # 记录停止日志
        logger.info("正在停止 RedisManager...")

        # 停止后台 PING 任务
        await self._health_watcher.stop()

        # 检查是否有后台任务（如延迟关闭旧连接的任务）
        if self._background_tasks:
            # 遍历所有后台任务并取消
            for task in self._background_tasks:
                task.cancel()

            # 等待所有任务完全退出
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

            # 清空任务集合，确保下次启动时集合为空（避免遗留任务）
            self._background_tasks.clear()

        # 获取当前活跃的 Redis 客户端
        client = self._client

        if client:
            # 异步关闭连接池
            await client.aclose()

            # 清空客户端引用
            self._client = None

        # 清除启动标志
        self._started = False

    async def _on_config_updated(self, _event: Event) -> None:
        """配置更新事件回调（事件驱动的热重载）

        该方法会在 Apollo 配置更新时被自动调用，触发热重载流程

        Args:
            _event (Event): 配置更新事件对象（包含事件元数据）
        """
        # 检查启动状态：若未启动，直接返回
        if not self._started:
            return

        logger.debug("检测到配置更新，检查是否需要重新加载 Redis...")

        try:
            # 触发热重载
            await self.reload()
        except Exception:
            # 热重载失败时的处理
            logger.exception("RedisManager 热重载失败")

    async def reload(self) -> None:
        """热重载 Redis 配置和连接

        该方法是热重载的核心逻辑，负责：
        1. 加载新配置
        2. 验证配置
        3. 创建新连接
        4. 原子切换
        5. 延迟关闭旧连接

        Raises:
            ValueError: 配置无效或缺失（仅在启动阶段抛出）
            RedisError: Redis 连接错误（仅在启动阶段抛出）
            AuthenticationError: Redis 认证失败（仅在启动阶段抛出）
        """
        # 获取热重载锁，避免并发热重载
        async with self._reload_lock:
            # 从 SettingsManager 加载 Redis 配置
            new_config = self._load_config()

            # 检查配置是否有效
            if not new_config:
                # 配置无效或缺失
                error_msg = "Redis 配置无效或缺失，无法加载。"

                # 配置降级策略：区分启动阶段和运行阶段
                if not self._started:
                    # 启动阶段：配置无效时抛出异常（快速失败）
                    raise ValueError(error_msg)
                else:
                    # 运行阶段：配置无效时保持旧配置（保持可用性）
                    logger.error(error_msg)
                    return

            # 刷新健康检查间隔
            self._refresh_health_interval()

            # 检查配置是否变更
            if self._config and self._config.model_dump() == new_config.model_dump():
                # 配置完全一致（所有字段都相同）
                if self._client:
                    # client 已存在，无需重连
                    logger.debug("Redis 配置未变更，跳过重连。")
                    return
                # 若 client 不存在（如热重载失败），继续创建连接

            try:
                # 创建并测试新的 Redis 客户端
                new_client = await self._init_and_test_client(new_config)

                # 保存旧客户端引用（用于延迟关闭）
                old_client = self._client

                # 原子更新客户端和配置
                self._client = new_client
                self._config = new_config

                # 检查是否有旧客户端需要关闭
                if old_client:
                    logger.info("计划关闭旧的 Redis 连接池...")

                    # 创建后台任务，延迟 5 秒后关闭旧连接
                    task = asyncio.create_task(self._safe_close_client(old_client))

                    # 添加任务到集合（用于停止时清理）
                    self._background_tasks.add(task)

                    # 注册完成回调：任务完成后自动从集合移除
                    task.add_done_callback(self._background_tasks.discard)

                # 记录成功日志
                logger.success("RedisManager 重新加载成功。")

            except Exception:
                # 热重载失败的处理
                if not self._started:
                    # 启动阶段：必须抛出异常
                    raise
                # 运行阶段：不抛出异常（已记录日志，保持旧配置）

    def _refresh_health_interval(self) -> None:
        """刷新健康检查间隔（从配置中读取）

        该方法从配置中心读取 redis_health_interval 配置，并动态更新健康检查器的轮询间隔
        """
        try:
            # 获取默认命名空间（如 "application"）
            # 从 SettingsManager.local（LocalConfig）中读取
            default_ns = self._settings.local.apollo_namespace

            # 获取配置段（ConfigSection）
            # section 包含该命名空间下的所有配置项（键值对）
            section = self._settings.get_section(default_ns)

            # 默认健康检查间隔（30 秒）
            interval = 30.0

            # 检查配置段是否存在
            if section:
                # 获取 system.config 配置项
                sys_conf = section.values.get("system.config")

                # 若 sys_conf 是字符串，尝试解析为 JSON
                if isinstance(sys_conf, str):
                    try:
                        sys_conf = json.loads(sys_conf)
                    except Exception:
                        pass

                # 若 sys_conf 是字典，提取健康检查间隔
                if isinstance(sys_conf, dict):
                    # 获取 timeouts 配置（字典）
                    timeouts = sys_conf.get("timeouts", {})

                    # 获取 redis_health_interval 配置（数值）
                    # 默认 30.0 秒（若未配置）
                    interval = float(timeouts.get("redis_health_interval", 30.0))

            # 动态更新健康检查器的轮询间隔
            self._health_watcher.update_intervals(interval, interval * 2)

        except Exception as e:
            # 刷新健康检查间隔失败
            logger.warning(f"刷新 Redis 健康检查间隔失败: {e}")

    async def _init_and_test_client(self, config: RedisConfig) -> redis.Redis:
        """创建并测试 Redis 客户端

        该方法执行以下操作：
        1. 解析 Redis URL
        2. 创建 Redis 客户端
        3. 测试连接（SET + GET）

        Args:
            config (RedisConfig): Redis 配置对象

        Returns:
            redis.Redis: 已测试的 Redis 客户端

        Raises:
            RedisError: Redis 连接错误（如连接断开、超时）
            AuthenticationError: Redis 认证失败（如密码错误）
            OSError: 系统错误（如网络不可达）
            Exception: 其他未预期的错误
        """
        try:
            # 解析 Redis URL
            url_options = redis.connection.parse_url(config.url)

            # 构造安全的日志字符串（不包含密码）
            safe_url_log = f"{url_options.get('host')}:{url_options.get('port')}/{url_options.get('db')}"

        except Exception:
            # URL 解析失败（如格式错误、缺少必需字段）
            safe_url_log = "无效"

        # 记录配置信息（安全日志，不包含密码）
        logger.info(f"正在应用新的 Redis 配置: {safe_url_log}")

        try:
            # 使用 redis.from_url() 创建客户端
            new_client = redis.from_url(
                config.url,
                max_connections=config.max_connections,
                socket_timeout=config.socket_timeout,
                socket_connect_timeout=config.socket_connect_timeout,
                retry_on_timeout=config.retry_on_timeout,
                encoding=config.encoding,
                decode_responses=config.decode_responses,
                health_check_interval=30
            )

            # 生成测试 key
            test_key = f"{AppConstants.DEFAULT_APP_NAME}:health:{int(time.time())}"

            # 执行 SET 命令
            await new_client.set(test_key, "1", ex=5)

            # 执行 GET 命令，验证是否能读取刚才设置的值
            await new_client.get(test_key)

            # 测试成功，返回客户端
            return new_client

        except (RedisError, AuthenticationError, OSError) as e:
            logger.error(f"连接/认证 Redis 失败: {e}")
            raise

        except Exception as e:
            logger.error(f"重新加载 Redis 时发生未预期的错误: {e}")
            raise

    async def _safe_close_client(self, client: redis.Redis):
        """安全关闭 Redis 客户端（延迟 5 秒）

        该方法会在后台异步执行，延迟 5 秒后关闭旧的 Redis 客户端

        Args:
            client (redis.Redis): 要关闭的 Redis 客户端
        """
        try:
            # 睡眠 5 秒，给活跃连接时间完成当前操作
            await asyncio.sleep(5)

            # 异步关闭 Redis 客户端
            await client.aclose()

            logger.debug("旧的 Redis 连接池已关闭。")

        except Exception as e:
            logger.warning(f"关闭旧 Redis 客户端时出错: {e}")

    def _load_config(self) -> Optional[RedisConfig]:
        """从配置中心加载 Redis 配置

        该方法从 SettingsManager 中提取 Redis 配置，并验证配置的有效性

        Returns:
            Optional[RedisConfig]: Redis 配置对象，若加载或验证失败则返回 None
        """
        try:
            # 获取默认命名空间（如 "application"）
            # 从 SettingsManager.local（LocalConfig）中读取
            default_ns = self._settings.local.apollo_namespace

            # 获取配置段（ConfigSection）
            section = self._settings.get_section(default_ns)

            # 初始化 Redis 配置数据（空字典）
            redis_conf_data = {}

            if section:
                # 获取 system.config 配置项
                sys_conf = section.values.get("system.config")

                # 检查 system.config 是否存在
                if sys_conf:
                    # 若 sys_conf 是字符串，尝试解析为 JSON
                    if isinstance(sys_conf, str):
                        try:
                            sys_conf = json.loads(sys_conf)
                        except json.JSONDecodeError:
                            pass

                    # 若 sys_conf 是字典，提取 redis 配置
                    if isinstance(sys_conf, dict):
                        # 获取 redis 配置（字典）
                        # 若不存在，返回空字典 {}（使用默认值）
                        redis_conf_data = sys_conf.get("redis", {})

            # 使用 Pydantic 验证配置
            return RedisConfig(**redis_conf_data)

        except ValidationError as e:
            logger.error(f"Redis 配置校验失败: {e}")
            return None
        except Exception as e:
            logger.error(f"加载 Redis 配置时发生未知错误: {e}")
            return None

    async def health_check(self) -> ResourceHealth:
        """被动健康检查（按需调用）

        该方法执行一次 PING 命令，验证 Redis 连接是否正常，并返回详细的健康状态信息

        Returns:
            ResourceHealth: 资源健康状态对象，包含状态、延迟、连接池信息等
        """
        # 获取当前活跃的 Redis 客户端
        client = self._client

        if not client:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.INIT
            )

        try:
            # 记录开始时间（高精度时钟）
            start_time = time.perf_counter()

            # 执行 PING 命令
            await client.ping()

            # 计算延迟（单位：毫秒）
            latency = (time.perf_counter() - start_time) * 1000

            # 初始化连接池信息（空字典）
            pool_info = {}

            # 检查连接池是否存在
            if client.connection_pool:
                # 获取已创建的连接数
                created_conns = getattr(client.connection_pool, "_created_connections", 0)

                # 构造连接池信息字典
                pool_info = {
                    "max_connections": client.connection_pool.max_connections,
                    "created_connections": created_conns
                }

            # 返回 HEALTHY 状态，包含详细信息：延迟、连接池信息
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.HEALTHY,
                details={
                    "latency_ms": round(latency, 2),
                    "pool_info": pool_info
                }
            )

        except Exception as e:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_error=str(e)
            )

    @property
    def client(self) -> redis.Redis:
        """获取 Redis 客户端（运行时检查）

        该属性提供了对 Redis 客户端的安全访问，确保客户端已初始化后才能使用

        Returns:
            redis.Redis: Redis 客户端实例

        Raises:
            RuntimeError: 若管理器尚未启动（client 为 None）
        """
        # 获取当前活跃的客户端
        client = self._client

        # 运行时检查：客户端是否已初始化
        if not client:
            raise RuntimeError("RedisManager 尚未启动。")

        # 返回客户端（已验证不为 None）
        return client

    def get_active_client(self) -> Optional[redis.Redis]:
        """获取活跃的 Redis 客户端（返回 Optional）

        该方法提供了对 Redis 客户端的宽松访问，允许客户端为 None（不抛出异常）

        Returns:
            Optional[redis.Redis]: Redis 客户端实例，若未启动则返回 None
        """
        # 直接返回当前客户端（可能为 None），不进行运行时检查（由调用者决定如何处理 None）
        return self._client

    async def acquire_lock(self, key: str, token: str, ttl_ms: int = 30000) -> bool:
        """尝试获取分布式锁 (原子操作)

        使用 Redis 的 SET key value NX PX ttl 命令实现原子加锁（为定时任务scheduler模块提供能力）
        这是实现 "定时任务自适应锁策略" 的基石：
        - 若 Redis 可用，调度器调用此方法获取分布式锁，保证集群中只有一个实例执行任务
        - 若 Redis 不可用，调度器降级为本地执行模式

        Args:
            key (str): 锁的键名，通常建议包含应用名和任务名（如 "fastchain:locks:daily_report"）
            token (str): 锁的持有者标识（通常是 UUID），用于防止误删他人的锁
            ttl_ms (int): 锁的自动过期时间（毫秒），防止持有锁的进程崩溃导致死锁。默认 30000ms

        Returns:
            bool: 成功获取锁返回 True，否则（被他人占用或 Redis 故障）返回 False
        """
        try:
            client = self.get_active_client()
            if not client:
                # Redis 不可用，视为获取锁失败
                return False

            # nx=True: 仅当键不存在时设置 (Not Exist)
            # px=ttl_ms: 设置过期时间（毫秒）
            # set 命令成功时返回 True (或 OK)，失败返回 None
            result = await client.set(key, token, nx=True, px=ttl_ms)
            return bool(result)

        except Exception as e:
            logger.warning(f"获取分布式锁失败: {e}")
            return False

    async def release_lock(self, key: str, token: str) -> bool:
        """释放分布式锁 (原子操作)

        使用 Lua 脚本确保仅当键存在且值匹配 token 时才删除，保证解锁的安全性（为定时任务scheduler模块提供能力）
        防止出现 "进程 A 的锁过期了，被进程 B 抢到，结果进程 A 醒来把进程 B 的锁删了" 的情况

        Args:
            key (str): 锁的键名
            token (str): 锁的持有者标识，必须与 acquire_lock 时的 token 一致

        Returns:
            bool: 成功释放返回 True，锁不存在或 token 不匹配返回 False
        """
        # Lua 脚本：原子性地检查并删除
        # KEYS[1]: 锁的键名
        # ARGV[1]: 锁的持有者 token
        lua_script = """
        if redis.call("get",KEYS[1]) == ARGV[1] then
            return redis.call("del",KEYS[1])
        else
            return 0
        end
        """
        try:
            client = self.get_active_client()
            if not client:
                return False

            # 执行 Lua 脚本
            result = await client.eval(lua_script, 1, key, token)
            # 1 表示成功删除，0 表示未删除（不存在或 token 不匹配）
            return result == 1

        except Exception as e:
            logger.warning(f"释放分布式锁失败: {e}")
            return False
