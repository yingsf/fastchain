from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from itertools import groupby
from typing import Any, Sequence, Optional

from loguru import logger

from .events import Event, EventBus, EventConstants
from .limits import ResourceLimiter
from ..config.constants import ResourceKind


class HealthStatus(str, Enum):
    """资源健康状态标准枚举

    定义了资源生命周期中的各种健康状态，用于健康检查、监控告警和流量控制
    状态定义遵循从"未知"到"彻底失败"的严重程度递增顺序

    状态语义：
    - UNKNOWN: 未执行过健康检查，或检查结果不确定（如超时）
    - INIT: 资源正在初始化，尚未完成启动流程
    - HEALTHY: 资源运行正常，所有功能可用
    - DEGRADED: 资源部分功能受损但仍可提供服务（如主从切换、使用缓存）
    - UNHEALTHY: 资源功能严重受损，不应接受新请求但可能仍在处理旧请求
    - DOWN: 资源完全不可用，已停止服务

    Attributes:
        self.is_healthy (bool): 便利属性，判断资源是否处于完全健康状态
        self.is_available (bool): 便利属性，判断资源是否可用（包括降级状态）

    Note:
        - 通过继承 str，枚举值可直接用于 JSON 序列化和日志记录
        - DEGRADED 状态允许系统在部分故障时继续提供服务，实现优雅降级
    """
    UNKNOWN = "unknown"
    INIT = "init"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DOWN = "down"

    @property
    def is_healthy(self) -> bool:
        """判断资源是否处于完全健康状态

        该属性用于快速判断资源是否可以承载全量流量，常用于负载均衡器的路由决策

        Returns:
            bool: 仅当状态为 HEALTHY 时返回 True

        Note:
            - DEGRADED 状态虽然可用，但不被视为完全健康，可能需要限流或降低权重
        """
        return self is HealthStatus.HEALTHY

    @property
    def is_available(self) -> bool:
        """判断资源是否可用（包括降级状态）

        该属性用于判断资源是否可以接受新请求，即使是降级模式
        用于熔断器、负载均衡器等组件的可用性判断

        Returns:
            bool: 当状态不为 DOWN 或 UNHEALTHY 时返回 True

        Note:
            - DEGRADED 状态下资源仍被视为可用，但可能需要降低流量或启用限流
            - INIT 和 UNKNOWN 状态也被视为可用，但实际流量控制应根据业务场景决策
        """
        return self not in (HealthStatus.DOWN, HealthStatus.UNHEALTHY)


@dataclass(slots=True)
class ResourceHealth:
    """资源健康检查结果快照

    该数据类封装了单次健康检查的完整结果，包括状态、错误信息、检查时间等，使用 slots=True 优化内存占用，适合高频健康检查场景

    Attributes:
        kind (ResourceKind): 资源类型，用于分类和监控分组
        name (str): 资源名称，用于唯一标识资源实例
        status (HealthStatus): 当前健康状态
        message (str | None): 可选的状态说明信息（如"数据库主从切换中"）
        last_error (str | None): 最后一次错误的详细信息（仅在状态异常时有值）
        details (dict[str, Any]): 额外的上下文信息（如最后成功时间、错误次数、延迟等）
        checked_at (float): 健康检查时间戳（使用 time.monotonic 防止系统时间调整影响）

    Note:
        - checked_at 使用 monotonic 时钟而非 time.time()，避免系统时间调整（如 NTP 同步）导致的时间倒退
        - details 字段允许扩展任意监控指标，便于与 Prometheus、Grafana 等监控系统集成
    """
    kind: ResourceKind
    name: str
    status: HealthStatus
    message: str | None = None
    last_error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    # 使用 monotonic 时钟作为默认值，防止系统时间调整导致的时间倒退问题，monotonic 保证时间单调递增，适合用于计算时间间隔和超时判断
    checked_at: float = field(default_factory=time.monotonic)


class Resource(ABC):
    """资源抽象基类，定义所有外部依赖资源的统一接口

    该基类为所有资源（数据库、缓存、配置中心、LLM 等）提供统一的生命周期管理接口
    包括启动、停止、健康检查和限流控制。通过抽象基类确保所有资源实现的一致性
    便于资源管理器（ResourceManager）进行统一编排

    核心设计原则：
    - 启动必须幂等：多次调用 start() 应该是安全的，不会导致重复初始化
    - 停止必须安全：stop() 不应抛出异常，确保资源清理流程的健壮性
    - 健康检查必须快速：避免阻塞主流程，建议设置合理的超时
    - 限流器可选：通过 limiter 属性支持动态设置资源限流策略

    Attributes:
        self._name (str): 资源名称，用于日志记录和唯一标识
        self._kind (ResourceKind): 资源类型，用于分类管理
        self._limiter (ResourceLimiter | None): 可选的资源限流器，用于控制并发访问
        self._startup_priority (int): 启动优先级，数值越小越先启动（如配置中心优先级为 0）

    Methods:
        start: 启动资源（抽象方法，子类必须实现）
        stop: 停止资源（抽象方法，子类必须实现）
        health_check: 执行健康检查（抽象方法，子类必须实现）
        can_accept: 判断当前是否可以接受新请求（基于限流器）
        acquire_slot: 获取资源槽位（用于限流控制）
        release_slot: 释放资源槽位（用于限流控制）
        get_metadata: 获取资源元数据（用于监控和事件发布）

    Note:
        - 启动优先级用于解决资源间的依赖关系，如配置中心必须在数据库之前启动
        - 限流器的集成允许在资源层面控制并发度，防止过载和级联故障
    """

    def __init__(
            self,
            name: str,
            *,
            kind: ResourceKind,
            limiter: ResourceLimiter | None = None,
            startup_priority: int = 10,
    ) -> None:
        """初始化资源

        Args:
            name (str): 资源名称，用于日志记录和唯一标识。建议使用有意义的名称（如 "user_db"、"redis_cache"）
            kind (ResourceKind): 资源类型，用于分类管理和监控分组
            limiter (ResourceLimiter | None): 可选的资源限流器。默认为 None（不启用限流）
            startup_priority (int): 启动优先级，数值越小越先启动。默认为 10（中等优先级）。建议值：配置中心=0, 数据库=10, 业务服务=20

        Note:
            - 启动优先级的设计目的是解决资源间的依赖关系，如配置中心必须在数据库之前启动
            - 相同优先级的资源会并发启动，不同优先级的资源会串行启动（低优先级组完成后才启动高优先级组）
        """
        self._name = name
        self._kind = kind
        self._limiter = limiter
        self._startup_priority = startup_priority

    @property
    def name(self) -> str:
        """资源名称

        Returns:
            str: 资源的唯一标识名称
        """
        return self._name

    @property
    def kind(self) -> ResourceKind:
        """资源类型

        Returns:
            ResourceKind: 资源的逻辑分类
        """
        return self._kind

    @property
    def startup_priority(self) -> int:
        """启动优先级

        Returns:
            int: 启动优先级数值，越小越先启动

        Note:
            - 该属性由 ResourceManager 用于确定资源的启动顺序
            - 相同优先级的资源会并发启动，不同优先级的资源会串行启动
        """
        return self._startup_priority

    @property
    def limiter(self) -> ResourceLimiter | None:
        """资源限流器

        Returns:
            ResourceLimiter | None: 当前资源的限流器实例，未设置时返回 None
        """
        return self._limiter

    @limiter.setter
    def limiter(self, value: ResourceLimiter | None) -> None:
        """设置资源限流器

        Args:
            value (ResourceLimiter | None): 新的限流器实例，传入 None 可禁用限流

        Note:
            - 允许动态设置限流器，便于在运行时调整资源的并发控制策略
        """
        self._limiter = value

    @abstractmethod
    async def start(self) -> None:
        """启动资源（抽象方法，子类必须实现）

        该方法负责资源的初始化和启动流程，如：
        - 建立数据库连接池
        - 初始化缓存客户端
        - 拉取配置中心的初始配置
        - 预热 LLM 模型

        实现要求：
        - 必须幂等：多次调用应该是安全的，不会导致重复初始化或资源泄漏
        - 允许抛出异常：启动失败时应抛出明确的异常，由 ResourceManager 捕获并记录
        - 支持优雅降级：对于非关键资源（如缓存），启动失败时可记录警告但不中断整体启动

        Raises:
            Exception: 启动失败时抛出异常，由 ResourceManager 捕获并记录

        Note:
            - ResourceManager 会根据 startup_priority 分组并发调用此方法
            - 相同优先级的资源会并发启动，启动失败会导致整个优先级组失败（Fail-fast 策略）
        """

    @abstractmethod
    async def stop(self) -> None:
        """停止资源（抽象方法，子类必须实现）

        该方法负责资源的清理和释放，如：
        - 关闭数据库连接池
        - 断开缓存连接
        - 停止后台轮询器
        - 释放 LLM 模型内存

        实现要求：
        - 绝对不能抛出异常：必须捕获所有异常并记录日志，确保停止流程不会中断
        - 必须幂等：多次调用应该是安全的，不会导致重复释放或资源泄漏
        - 支持优雅关闭：应等待当前正在执行的请求完成，避免数据损坏或请求中断

        Note:
            - ResourceManager 会根据 startup_priority 逆序分组并发调用此方法（高优先级先停止）
            - 停止操作使用 Best-effort 策略，即使部分资源停止失败也不会影响其他资源的停止
            - 使用 return_exceptions=True 确保单个资源的停止异常不会导致整体停止流程中断
        """

    @abstractmethod
    async def health_check(self) -> ResourceHealth:
        """执行健康检查（抽象方法，子类必须实现）

        该方法返回资源的当前健康状态，用于：
        - 监控系统的健康度指标采集（如 Prometheus）
        - 负载均衡器的路由决策（如 Kubernetes readiness probe）
        - 熔断器的状态判断（如连续失败后熔断）

        实现要求：
        - 必须快速返回：建议在 1 秒内完成，避免阻塞主流程
        - 不应抛出异常：异常应被捕获并转换为 DOWN 状态
        - 提供详细信息：通过 ResourceHealth.details 提供延迟、错误次数等监控指标

        Returns:
            ResourceHealth: 包含状态、错误信息、检查时间等的健康检查结果

        Note:
            - ResourceManager 会定期调用此方法（如每 10 秒一次）以生成健康快照
            - 健康检查失败时，ResourceManager 会将异常转换为 DOWN 状态而不是向上传播
        """

    def can_accept(self) -> bool:
        """判断当前是否可以接受新请求（基于限流器）

        该方法用于流量控制和过载保护，在接受新请求前调用以判断资源是否有足够的容量

        Returns:
            bool: 若未设置限流器或限流器允许，返回 True；否则返回 False

        Note:
            - 该方法不会阻塞，仅检查当前状态，适合用于快速的预检查
            - 若返回 False，调用方应拒绝请求或选择其他资源实例（如负载均衡）
        """
        # 若未设置限流器，则始终允许接受新请求
        if self._limiter is None:
            return True
        # 若设置了限流器，则委托给限流器判断（如检查并发数、速率限制等）
        return self._limiter.can_accept()

    async def acquire_slot(self) -> None:
        """获取资源槽位（用于限流控制）

        该方法用于在处理请求前获取资源槽位，若当前无可用槽位则阻塞等待，通常与 release_slot() 配合使用，确保并发度不超过限制

        典型用法：
            await resource.acquire_slot()
            try:
                await resource.do_work()
            finally:
                resource.release_slot()

        Note:
            - 若未设置限流器，该方法为空操作（立即返回）
            - 若设置了限流器，该方法会阻塞直到获取到槽位或超时
            - 必须与 release_slot() 配对使用，否则会导致槽位泄漏和资源饥饿
        """
        # 若设置了限流器，则调用限流器的 acquire 方法（可能阻塞等待）
        if self._limiter is not None:
            await self._limiter.acquire()

    def release_slot(self) -> None:
        """释放资源槽位（用于限流控制）

        该方法用于在请求处理完成后释放槽位，允许其他等待的请求获取槽位

        Note:
            - 若未设置限流器，该方法为空操作
            - 该方法不会抛出异常，即使重复释放也是安全的（由限流器内部处理）
            - 建议在 finally 块中调用，确保即使请求处理失败也能释放槽位
        """
        # 若设置了限流器，则调用限流器的 release 方法
        if self._limiter is not None:
            self._limiter.release()

    def get_metadata(self) -> dict[str, Any]:
        """获取资源元数据（用于监控和事件发布）

        该方法返回资源的基本元数据，用于：
        - 事件发布（如 resource.started 事件的 payload）
        - 监控指标标签（如 Prometheus 的 resource_name、resource_kind 标签）
        - 日志结构化字段（如 ELK 的 structured logging）

        Returns:
            dict[str, Any]: 包含名称、类型、优先级的元数据字典

        Note:
            - 该方法可被子类覆盖以添加更多元数据（如连接池大小、缓存命中率等）
        """
        return {
            EventConstants.KEY_RESOURCE_NAME: self._name,
            # 转换为字符串以便 JSON 序列化
            EventConstants.KEY_RESOURCE_KIND: self._kind.value,
            "priority": self._startup_priority,
        }


class ResourceRegistry:
    """资源注册表，用于管理所有资源实例的注册和查询

    该类提供资源的集中管理，支持按类型和名称查询资源实例。通过键值对存储（(kind, name) -> Resource）确保资源的唯一性

    核心功能：
    - 资源注册：允许动态注册新资源（如在插件加载时）
    - 资源查询：支持按 (kind, name) 精确查询或按 name 模糊查询
    - 资源枚举：返回所有已注册资源的列表

    Attributes:
        self._resources (dict[tuple[ResourceKind, str], Resource]): 资源存储字典，键为 (kind, name) 元组

    Methods:
        register: 注册资源实例
        get: 按 (kind, name) 精确查询资源
        get_by_name: 按 name 模糊查询资源（遍历所有资源）
        all: 返回所有已注册资源的列表

    Note:
        - 当前实现未加锁，假设资源注册仅在启动阶段进行（单线程）
        - 若需支持运行时动态注册（多线程），需添加线程锁保护 _resources 字典
    """

    def __init__(self) -> None:
        """初始化资源注册表

        创建一个空的资源存储字典，键为 (kind, name) 元组，值为 Resource 实例
        """
        # 使用 (kind, name) 元组作为键，确保不同类型的资源可以使用相同的名称
        # 例如：可以同时存在 (DATABASE, "cache") 和 (CACHE, "cache")
        self._resources: dict[tuple[ResourceKind, str], Resource] = {}

    def register(self, resource: Resource) -> None:
        """注册资源实例

        若资源已存在（相同 kind 和 name），则覆盖旧实例并记录警告日志
        这种设计允许插件系统动态替换资源实现（如从 SQLite 切换到 PostgreSQL）

        Args:
            resource (Resource): 要注册的资源实例

        Note:
            - 当前实现允许覆盖，适用于插件热重载等场景
            - 若需严格模式（禁止覆盖），可取消注释 ValueError 抛出逻辑
            - 建议在生产环境启用严格模式，防止意外覆盖导致的配置错误
        """
        # 构造复合键：(kind, name)
        key = (resource.kind, resource.name)

        # 检查资源是否已注册
        if key in self._resources:
            # 宽松模式：允许覆盖并记录警告日志
            # 这适用于插件系统、热重载等需要动态替换资源的场景
            # 若需严格模式，可抛出 ValueError 阻止覆盖
            # raise ValueError(f"资源 {resource.name} ({resource.kind}) 已经注册")
            logger.warning(f"资源 '{resource.name}' 已经注册。正在覆盖。")

        # 注册（或覆盖）资源
        self._resources[key] = resource

    def get(self, kind: ResourceKind, name: str) -> Resource | None:
        """按 (kind, name) 精确查询资源

        该方法用于精确查询特定类型和名称的资源，时间复杂度为 O(1)

        Args:
            kind (ResourceKind): 资源类型
            name (str): 资源名称

        Returns:
            Resource | None: 若资源存在则返回实例，否则返回 None

        Note:
            - 这是最高效的查询方式，适合已知 kind 和 name 的场景
        """
        return self._resources.get((kind, name))

    def get_by_name(self, name: str) -> Resource | None:
        """按 name 模糊查询资源（遍历所有资源）

        该方法在不知道资源类型的情况下查询资源，通过遍历所有资源实现，时间复杂度为 O(n)，适合资源数量较少或查询频率不高的场景

        Args:
            name (str): 资源名称。

        Returns:
            Resource | None: 若找到第一个匹配的资源则返回实例，否则返回 None

        Note:
            - 若多个不同类型的资源使用相同名称，该方法仅返回第一个匹配的资源
            - 对于高频查询场景，建议维护 name -> Resource 的索引以提升性能
        """
        # 遍历所有资源，返回第一个名称匹配的资源
        for res in self._resources.values():
            if res.name == name:
                return res
        return None

    def all(self) -> Sequence[Resource]:
        """返回所有已注册资源的列表（未排序）

        该方法用于资源枚举，如批量健康检查、全量启停等场景

        Returns:
            Sequence[Resource]: 所有资源的列表（顺序不确定）

        Note:
            - 返回的列表是快照，不会因后续的注册操作而改变
            - 若需按优先级排序，调用方应自行排序（如 ResourceManager.start_all）
        """
        return list(self._resources.values())


class ResourceManager:
    """资源管理器，负责所有资源的编排和生命周期管理

    该类是资源管理的核心组件，提供统一的资源注册、启停、健康检查等功能。通过分层启停策略解决资源间的依赖关系问题

    核心功能：
    - 分层并发启动：按优先级分组启动资源，组与组之间串行，组内并发（Fail-fast）
    - 分层并发停止：按优先级逆序分组停止资源，组与组之间串行，组内并发（Best-effort）
    - 健康快照：并发收集所有资源的健康状态，用于监控和告警
    - 事件发布：在资源启停时发布事件，用于审计和外部系统集成

    分层启停策略：
    1. 启动阶段：
       - 按 startup_priority 从小到大分组
       - 先启动配置中心（priority=0），完成后再启动数据库（priority=5）
       - 确保底层基础设施先于上层业务服务启动
       - 组内并发启动，任一失败则整个组失败（Fail-fast）

    2. 停止阶段：
       - 按 startup_priority 从大到小分组（逆序）
       - 先停止业务服务（priority=20），完成后再停止数据库（priority=5）
       - 确保上层服务先于底层基础设施停止，避免依赖缺失
       - 组内并发停止，即使部分失败也继续停止其他资源（Best-effort）

    Attributes:
        self._registry (ResourceRegistry): 资源注册表实例
        self._event_bus (EventBus | None): 可选的事件总线，用于发布资源生命周期事件

    Methods:
        register: 注册资源实例
        get_resource: 按名称查询资源（不存在则抛出异常）
        get_resource_or_none: 按名称查询资源（不存在则返回 None）
        start_all: 分层并发启动所有资源
        stop_all: 分层并发停止所有资源
        get_health_snapshot: 并发收集所有资源的健康状态

    Note:
        - 分层启停策略的关键是 groupby 函数，它要求输入列表已按 key 排序
        - Fail-fast 策略用于启动阶段，确保关键资源启动失败时不会继续启动上层服务
        - Best-effort 策略用于停止阶段，确保即使部分资源停止失败也能继续停止其他资源
    """

    def __init__(
            self,
            registry: ResourceRegistry | None = None,
            event_bus: EventBus | None = None,
    ) -> None:
        """初始化资源管理器

        Args:
            registry (ResourceRegistry | None): 资源注册表实例。默认为 None（自动创建）
            event_bus (EventBus | None): 事件总线实例，用于发布资源生命周期事件。默认为 None（不发布事件）

        Note:
            - 若未提供 registry，则自动创建一个新的 ResourceRegistry 实例
            - 事件总线是可选的，未设置时资源启停不会发布事件（适合测试环境）
        """
        # 若未提供注册表，则创建一个新的空注册表
        self._registry = registry or ResourceRegistry()
        self._event_bus = event_bus

    @property
    def registry(self) -> ResourceRegistry:
        """资源注册表

        Returns:
            ResourceRegistry: 当前资源管理器使用的注册表实例

        Note:
            - 暴露注册表允许外部直接访问底层存储，便于高级查询和调试
        """
        return self._registry

    def register(self, resource: Resource) -> None:
        """注册资源实例

        该方法是 ResourceRegistry.register 的便捷封装，允许通过管理器直接注册资源

        Args:
            resource (Resource): 要注册的资源实例

        Note:
            - 若资源已存在（相同 kind 和 name），则覆盖旧实例并记录警告日志
        """
        self._registry.register(resource)

    def get_resource(self, name: str) -> Resource:
        """按名称查询资源（不存在则抛出异常）

        该方法用于强制获取资源，若资源不存在则抛出 KeyError 并提供可用资源列表

        Args:
            name (str): 资源名称

        Returns:
            Resource: 找到的资源实例

        Raises:
            KeyError: 当资源不存在时，抛出异常并列出所有可用资源

        Note:
            - 该方法适合在配置阶段使用，确保所有必需的资源都已注册
            - 异常信息中包含可用资源列表，便于快速定位配置错误
        """
        res = self._registry.get_by_name(name)
        if not res:
            # 获取所有可用资源的名称列表
            available = [r.name for r in self._registry.all()]
            raise KeyError(f"资源 '{name}' 未找到。可用资源: {available}")
        return res

    def get_resource_or_none(self, name: str) -> Optional[Resource]:
        """按名称查询资源（不存在则返回 None）

        该方法是 get_resource 的非抛出版本，适合可选资源的查询场景

        Args:
            name (str): 资源名称

        Returns:
            Optional[Resource]: 若资源存在则返回实例，否则返回 None

        Note:
            - 该方法适合在运行时使用，如根据配置动态选择资源实现
        """
        return self._registry.get_by_name(name)

    async def _emit(self, event_type: str, payload: dict[str, Any]) -> None:
        """发布资源生命周期事件（内部方法）

        该方法封装了事件发布逻辑，仅在配置了事件总线时才发布事件

        Args:
            event_type (str): 事件类型（如 "resource.started"、"resource.stopped"）
            payload (dict[str, Any]): 事件负载（通常包含资源元数据）

        Note:
            - 事件发布是异步的，不会阻塞资源启停流程
            - 若事件总线未配置，该方法为空操作（不会产生额外开销）
            - 事件可被外部系统订阅，用于审计、监控、告警等场景
        """
        # 仅在配置了事件总线时才发布事件
        if self._event_bus:
            await self._event_bus.publish(Event(type=event_type, payload=payload))

    async def _start_one(self, res: Resource) -> None:
        """启动单个资源（内部方法）

        该方法封装了单个资源的启动流程，包括日志记录、异常处理和事件发布

        Args:
            res (Resource): 要启动的资源实例

        Raises:
            Exception: 若资源启动失败，向上传播异常（由 start_all 的 gather 捕获）

        Note:
            - 启动成功后发布 "resource.started" 事件
            - 启动失败后发布 "resource.start_failed" 事件并向上传播异常
            - 异常传播确保了 Fail-fast 策略的实现（组内任一失败则整个组失败）
        """
        try:
            logger.debug(f"正在启动资源: {res.name} (Priority: {res.startup_priority})...")
            # 调用资源的启动方法（可能抛出异常）
            await res.start()
            logger.info(f"资源已启动: {res.name}")
            # 发布启动成功事件，包含资源元数据
            await self._emit(EventConstants.RESOURCE_STARTED, res.get_metadata())
        except Exception as exc:
            # 启动失败时记录错误日志
            logger.error(f"启动资源失败 {res.name}: {exc}")
            # 发布启动失败事件，包含资源元数据和错误信息
            await self._emit(
                EventConstants.RESOURCE_START_FAILED,
                {**res.get_metadata(), EventConstants.KEY_ERROR: str(exc)}
            )
            # 向上传播异常，由 start_all 的 gather 捕获（Fail-fast 策略）
            raise

    async def _stop_one(self, res: Resource) -> None:
        """停止单个资源（内部方法）

        该方法封装了单个资源的停止流程，包括日志记录、异常处理和事件发布
        与 _start_one 不同，该方法捕获所有异常而不向上传播，确保 Best-effort 策略

        Args:
            res (Resource): 要停止的资源实例

        Note:
            - 停止成功后发布 "resource.stopped" 事件
            - 停止失败时记录警告日志但不抛出异常（Best-effort 策略）
            - 不向上传播异常确保了即使部分资源停止失败也能继续停止其他资源
        """
        try:
            # 调用资源的停止方法（不应抛出异常，但仍需防御）
            await res.stop()
            logger.info(f"资源已停止: {res.name}")
            # 发布停止成功事件
            await self._emit(EventConstants.RESOURCE_STOPPED, res.get_metadata())
        except Exception as exc:
            logger.warning(f"停止资源时出错 {res.name}: {exc}")

    async def start_all(self) -> None:
        """分层并发启动所有资源

        该方法实现了基于优先级的分层启动策略，解决资源间的依赖关系问题：
        1. 按 startup_priority 从小到大对所有资源排序
        2. 将相同优先级的资源归为一组
        3. 组与组之间串行执行（await），确保低优先级组完全启动后再启动高优先级组
        4. 组内资源并发执行（gather），提升启动效率
        5. 任意资源启动失败则整个组失败（Fail-fast），阻止上层服务启动

        启动顺序示例：
            Priority 0: [apollo_config]           -> 配置中心先启动
            Priority 5: [user_db, order_db]       -> 数据库并发启动（依赖配置）
            Priority 10: [redis_cache]            -> 缓存启动（依赖数据库和配置）
            Priority 20: [api_server, worker]     -> 业务服务并发启动（依赖所有底层设施）

        Raises:
            Exception: 若任意资源启动失败，向上传播异常并停止后续组的启动

        Note:
            - groupby 要求输入列表已排序，否则分组结果不正确
            - Fail-fast 策略确保关键资源（如配置中心）启动失败时不会继续启动上层服务
            - 组内并发启动通过 gather 实现，任一失败会导致整个 gather 抛出异常
        """
        # 获取所有已注册的资源
        resources = self._registry.all()

        # 若没有资源，直接返回（避免不必要的日志输出）
        if not resources:
            return

        # 步骤 1：按 startup_priority 从小到大排序
        # 这是 groupby 的前提条件，确保相同优先级的资源在列表中相邻
        sorted_resources = sorted(resources, key=lambda r: r.startup_priority)

        logger.info(f"开始启动 {len(resources)} 个资源 (分层策略)...")

        # 步骤 2：使用 groupby 按优先级分组
        # groupby 返回迭代器，需要立即转换为列表以避免迭代器失效
        for priority, group_iter in groupby(sorted_resources, key=lambda r: r.startup_priority):
            # 将迭代器转换为列表（groupby 的迭代器仅能遍历一次）
            group = list(group_iter)
            group_names = [r.name for r in group]

            # 记录当前正在启动的优先级层级和资源列表
            logger.info(f"🚀 启动优先级层级 [{priority}]: {group_names}")

            # 步骤 3：组内并发启动（Fail-fast）
            # gather 会并发执行所有 _start_one 协程
            # 若任意协程抛出异常，gather 会立即抛出该异常（默认行为）
            # 这实现了 Fail-fast 策略：组内任意资源启动失败，整个组失败
            await asyncio.gather(*(self._start_one(r) for r in group))

        logger.success("所有资源层级已完成启动。")

    async def stop_all(self) -> None:
        """分层并发停止所有资源（Phased Shutdown）

        该方法实现了基于优先级的分层停止策略，与启动顺序相反：
        1. 按 startup_priority 从大到小对所有资源排序（逆序）
        2. 将相同优先级的资源归为一组
        3. 组与组之间串行执行（await），确保高优先级组完全停止后再停止低优先级组
        4. 组内资源并发执行（gather），提升停止效率
        5. 即使部分资源停止失败也继续停止其他资源（Best-effort）

        停止顺序示例：
            Priority 20: [api_server, worker]     -> 业务服务先停止
            Priority 10: [redis_cache]            -> 缓存停止
            Priority 5: [user_db, order_db]       -> 数据库并发停止
            Priority 0: [apollo_config]           -> 配置中心最后停止

        逆序停止的设计目的：
        - 先停止业务服务，避免新请求进入系统
        - 再停止底层基础设施（数据库、缓存），确保业务服务停止时依赖仍可用
        - 最后停止配置中心，确保停止过程中仍可读取配置

        Note:
            - 使用 return_exceptions=True 确保单个资源停止失败不会影响其他资源的停止
            - Best-effort 策略允许系统在部分资源停止失败时仍能优雅关闭
            - 停止操作不应抛出异常（由 Resource.stop 的契约保证），但仍需防御性处理
        """
        # 获取所有已注册的资源
        resources = self._registry.all()

        # 若没有资源，直接返回
        if not resources:
            return

        # 步骤 1：按 startup_priority 从大到小排序（逆序）
        # 逆序确保高优先级的业务服务先于低优先级的基础设施停止
        sorted_resources = sorted(resources, key=lambda r: r.startup_priority, reverse=True)

        logger.info(f"开始停止 {len(resources)} 个资源 (逆序分层)...")

        # 步骤 2：使用 groupby 按优先级分组（注意：输入已逆序排序）
        for priority, group_iter in groupby(sorted_resources, key=lambda r: r.startup_priority):
            # 将迭代器转换为列表
            group = list(group_iter)
            group_names = [r.name for r in group]

            # 记录当前正在停止的优先级层级和资源列表
            logger.info(f"🛑 停止优先级层级 [{priority}]: {group_names}")

            # 步骤 3：组内并发停止（Best-effort）
            # return_exceptions=True 确保单个资源停止失败不会导致整个 gather 失败
            # 这实现了 Best-effort 策略：即使部分资源停止失败，也继续停止其他资源
            await asyncio.gather(*(self._stop_one(r) for r in group), return_exceptions=True)

        logger.success("所有资源层级已完成停止。")

    async def get_health_snapshot(self) -> list[ResourceHealth]:
        """并发收集所有资源的健康状态

        该方法用于生成系统级的健康快照，常用于：
        - 监控系统的健康度指标采集（如 Prometheus /health 端点）
        - 负载均衡器的健康检查（如 Kubernetes liveness/readiness probe）
        - 运维大盘的实时状态展示（如 Grafana Dashboard）

        并发策略：
        - 使用 gather 并发调用所有资源的 health_check 方法，提升检查效率
        - 若某个资源的 health_check 抛出异常，捕获异常并转换为 DOWN 状态
        - 不使用 return_exceptions=True，而是在循环中单独捕获，以便记录详细错误

        Returns:
            list[ResourceHealth]: 所有资源的健康状态列表（顺序与注册顺序一致）

        Note:
            - 健康检查应快速返回（建议 < 1 秒），避免阻塞监控系统的查询
            - 若资源的 health_check 抛出异常，会被转换为 DOWN 状态而不是向上传播
            - 返回的快照是瞬时状态，不保证在返回时仍然有效（如资源可能已停止）
        """
        results: list[ResourceHealth] = []

        # 遍历所有已注册的资源
        for res in self._registry.all():
            try:
                # 调用资源的健康检查方法（可能抛出异常）
                health = await res.health_check()
            except Exception as exc:
                # 若健康检查抛出异常，转换为 DOWN 状态
                # 这防止单个资源的健康检查异常导致整个快照收集失败
                health = ResourceHealth(
                    kind=res.kind,
                    name=res.name,
                    status=HealthStatus.DOWN,
                    # 标识这是由异常转换而来的
                    message="health_check exception",
                    last_error=str(exc)
                )
            results.append(health)

        return results
