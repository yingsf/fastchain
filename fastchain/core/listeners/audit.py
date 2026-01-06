from __future__ import annotations

from typing import override

from loguru import logger

from ...core.config.constants import ResourceKind
from ...core.db.relational import RelationalDBManager, AuditLog
from ...core.resources.base import Resource, ResourceHealth, HealthStatus
from ...core.resources.events import Event, EventBus, EventConstants


class AuditEventListener(Resource):
    """LLM 审计日志持久化监听器（事件驱动架构）

    负责监听 LLM 调用产生的审计事件（成功或失败），并将事件负载异步写入关系型数据库。
    采用事件驱动设计，解耦 LLM 调用层与审计存储层。

    架构升级：
    - 继承自 Resource，纳入 ResourceManager 统一管理
    - 支持健康检查和生命周期管理
    - 异步化 start/stop 接口
    - 完全适配 EventConstants，消除 Magic Strings

    核心设计特点：
        - 事件驱动：通过 EventBus 订阅 'llm.audit.*' 事件，实现松耦合
        - 异步持久化：使用 SQLAlchemy 异步会话，避免阻塞 LLM 调用链路
        - 防御式编程：对事件负载中的字段提供默认值，防止数据库约束冲突
        - 降级策略：数据库写入失败时，将 payload 记录到日志，防止数据丢失

    Attributes:
        db_manager (RelationalDBManager): 数据库管理器，提供会话工厂
        event_bus (EventBus): 事件总线，用于订阅和发布事件
        _running (bool): 运行状态标志

    Methods:
        start: 启动监听器并订阅审计事件
        stop: 停止监听器
        health_check: 检查组件健康状态
    """

    def __init__(self, db_manager: RelationalDBManager, event_bus: EventBus):
        """初始化审计事件监听器

        Args:
            db_manager (RelationalDBManager): 关系型数据库管理器实例
            event_bus (EventBus): 事件总线实例
        """
        # 初始化资源基类
        # Kind 设置为 INFRA 或 COMPONENT，优先级设为 20（需在 DB 启动后启动）
        super().__init__(
            name="audit-listener",
            kind=ResourceKind.INFRA,
            startup_priority=20
        )
        self.db_manager = db_manager
        self.event_bus = event_bus
        self._running = False

    @override
    async def start(self) -> None:
        """启动审计监听器并订阅相关事件（幂等）

        Warning:
            必须在 RelationalDBManager 启动之后调用此方法，否则会话工厂不可用
            (ResourceManager 的优先级机制保证了这一点)
        """
        if self._running:
            return

        logger.info(f"正在启动资源: {self.name}...")

        # 订阅成功事件：LLM 调用成功时发布
        # 使用常量 EventConstants.LLM_AUDIT_SUCCESS
        self.event_bus.subscribe(EventConstants.LLM_AUDIT_SUCCESS, self._on_audit_event)

        # 订阅失败事件：LLM 调用失败时发布
        # 使用常量 EventConstants.LLM_AUDIT_ERROR
        self.event_bus.subscribe(EventConstants.LLM_AUDIT_ERROR, self._on_audit_event)

        self._running = True
        logger.info("审计事件监听器已启动。")

    @override
    async def stop(self) -> None:
        """停止审计监听器（优雅停机）"""
        self._running = False
        logger.info("审计事件监听器已停止。")

    @override
    async def health_check(self) -> ResourceHealth:
        """执行健康检查

        检查依赖的数据库管理器是否健康，以及自身是否处于运行状态。
        """
        if not self._running:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.DOWN,
                message="监听器未运行"
            )

        # 检查依赖的 DB Manager 是否健康
        # 这是一个简单的级联检查，确保审计功能可用
        try:
            _ = self.db_manager.session_maker
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.HEALTHY,
                message="运行中且数据库已连接"
            )
        except Exception as e:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.DEGRADED,
                message="数据库管理器依赖问题",
                last_error=str(e)
            )

    async def _on_audit_event(self, event: Event) -> None:
        """审计事件的异步回调处理器（核心业务逻辑）

        Args:
            event (Event): 审计事件对象
        """
        if not self._running:
            return

        # 提取事件负载
        payload = event.payload or {}

        # 提取 trace_id
        trace_id = payload.get(EventConstants.KEY_TRACE_ID, "unknown")

        try:
            # ==================== 1. 构造 ORM 对象 ====================
            log_entry = AuditLog(
                trace_id=str(trace_id),
                model_alias=str(payload.get("model_alias", "unknown")),
                scene=str(payload.get("scene", "default")),
                status=str(payload.get("status", "unknown")),
                latency_ms=float(payload.get("latency_ms", 0.0)),
                input_data={
                    "messages": payload.get("input"),
                    "metadata": payload.get("metadata", {})
                },
                output_data=str(payload.get("output", "")),

                # 优先使用标准 Key，兼容旧 Key
                error_msg=str(
                    payload.get(EventConstants.KEY_ERROR) or
                    payload.get("error_message") or
                    ""
                ) or None,

                meta_info={
                    "error_type": payload.get("error_type"),
                    "provider": payload.get("provider"),
                },
                prompt_tokens=int(payload.get("prompt_tokens", 0)),
                completion_tokens=int(payload.get("completion_tokens", 0)),
                total_tokens=int(payload.get("total_tokens", 0)),
                ttft_ms=float(payload.get("ttft_ms")) if payload.get("ttft_ms") else None
            )

            # ==================== 2. 写入数据库 ====================
            session_maker = self.db_manager.session_maker

            async with session_maker() as session, session.begin():
                session.add(log_entry)

        except Exception:
            # ==================== 3. 异常降级策略 ====================
            logger.exception(f"持久化审计日志失败 [TraceID: {trace_id}]")
            logger.error(f"丢弃的审计负载: {payload}")
