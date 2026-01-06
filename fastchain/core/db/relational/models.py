from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from sqlalchemy import String, Float, Text, JSON, DateTime, func, Integer, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy 2.0 声明式基类

    所有 ORM 模型类都应继承自此基类，以便统一管理表结构和元数据

    Note:
        - 这里保持为空实现即可；核心价值在于聚合声明式模型的元数据
        - 若项目未来需要统一命名约定、type decorator 等，可在此基类扩展
    """
    pass


class AuditLog(Base):
    """LLM 审计日志表（核心业务数据模型）。

    用于记录所有 LLM 调用的完整生命周期信息
    包括：追踪 ID、模型别名、业务场景、成功/失败状态、端到端耗时、输入输出上下文、异常信息以及 Token 统计等
    支持全链路追踪和多维度性能分析

    典型用途包括：
        - 线上问题排障：通过 trace_id 还原一次请求的完整上下文与错误栈
        - 运营分析：按 scene 聚合统计时延、成功率、TTFT 等指标
        - 成本核算：基于 Token 维度进行费用/资源消耗评估

    设计考量：
        - 所有时间戳字段使用 timezone-aware 类型，确保跨时区一致性
        - 组合索引 (scene, ttft_ms) 专为按场景统计首字延迟而优化
        - JSON 字段用于存储灵活的元数据，便于后续扩展而无需修改表结构

    Attributes:
        id (int): 主键，自增 ID
        trace_id (str): 全链路追踪 ID，用于关联分布式系统中的上下游请求
        created_at (datetime): 记录创建时间（带时区），服务端默认值为当前时间
        model_alias (str): 模型别名，用于区分不同的 LLM 模型或版本
        scene (str): 业务场景标识（如 "chat"、"summary" 等），用于分场景统计
        status (str): 调用状态，取值为 "success" 或 "error"
        latency_ms (float): 总耗时（毫秒），包含网络开销和模型处理时间
        input_data (Dict[str, Any]): 完整的输入上下文（JSON 格式），如 prompt、参数等
        output_data (str): 模型生成的输出内容（纯文本），可为空（调用失败时）
        error_msg (str | None): 错误信息，仅在 status="error" 时有值
        meta_info (Dict[str, Any]): 扩展元数据（JSON 格式），如模型版本、API 响应头等
        prompt_tokens (int): 输入 Token 数量，用于成本核算
        completion_tokens (int): 输出 Token 数量，用于成本核算
        total_tokens (int): 总 Token 数（prompt + completion），用于快速统计
        ttft_ms (float | None): Time To First Token（首字延迟，毫秒），流式响应的关键性能指标

    Methods:
        __repr__: 返回对象的字符串表示，用于调试和日志输出。

    Note:
        - 组合索引 idx_scene_ttft 可显著提升 "按场景查询平均首字延迟" 的查询性能。
        - JSON 字段在 PostgreSQL 中会使用 JSONB 类型，支持高效的键值查询。
    """
    __tablename__ = "audit_logs"

    # 主键：使用自增 ID 而非 UUID，减少索引存储开销和插入时的随机 I/O
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # 追踪 ID：建立索引以支持快速的全链路查询（如 APM 工具中的 trace 搜索）
    trace_id: Mapped[str] = mapped_column(String(64), index=True, comment="全链路追踪ID")

    # 时间戳：使用 timezone=True 确保跨时区部署时的一致性；
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        # 索引支持按时间范围查询（如 "最近 24 小时的调用日志"）
        index=True
    )

    # 模型标识：用于区分不同模型（如 deepseek、qwen 等），支持多模型统计分析
    model_alias: Mapped[str] = mapped_column(String(64), comment="模型别名")

    # 场景标识：用于业务层面的多维度统计
    scene: Mapped[str] = mapped_column(String(64), default="default", comment="业务场景")

    # 状态字段：枚举类型（success/error），便于快速统计成功率
    status: Mapped[str] = mapped_column(String(20), comment="success/error")

    # 总耗时：包含网络往返和模型处理时间，用于 SLA 监控
    latency_ms: Mapped[float] = mapped_column(Float, comment="耗时(ms)")

    # 输入数据：存储完整的 prompt 和参数，便于问题复现和调试，使用 JSON 而非 Text 是为了支持结构化查询
    input_data: Mapped[Dict[str, Any]] = mapped_column(JSON, comment="完整输入上下文")

    # 输出数据：存储模型生成内容，失败时为 NULL
    output_data: Mapped[str] = mapped_column(Text, nullable=True, comment="模型生成内容")

    # 错误信息：仅在调用失败时记录，避免占用额外空间
    error_msg: Mapped[str | None] = mapped_column(Text, nullable=True)

    # 元数据：扩展字段，用于存储非标准化的额外信息（如模型版本、API 响应头等），default=dict 确保默认值为空字典而非 None，简化应用层逻辑
    meta_info: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict, comment="Token消耗等元数据")

    # Token 统计字段：用于成本核算和配额管理，冗余存储 total_tokens（而非计算字段）是为了避免聚合查询时的重复计算开销
    prompt_tokens: Mapped[int] = mapped_column(Integer, default=0, comment="输入Token数")
    completion_tokens: Mapped[int] = mapped_column(Integer, default=0, comment="输出Token数")
    total_tokens: Mapped[int] = mapped_column(Integer, default=0, comment="总Token数")

    # 首字延迟（TTFT）：流式响应的关键性能指标，用于优化用户体验，允许为 NULL 是因为非流式调用没有此指标
    ttft_ms: Mapped[float | None] = mapped_column(Float, nullable=True, comment="首字延迟")

    # 组合索引：优化 "按场景统计首字延迟" 的查询场景
    # 例如：SELECT AVG(ttft_ms) FROM audit_logs WHERE scene='chat' ORDER BY ttft_ms;
    # 将 scene 放在索引第一列，是因为 WHERE 条件会先过滤场景，然后在该场景内排序
    __table_args__ = (
        Index("idx_scene_ttft", "scene", "ttft_ms"),
    )

    def __repr__(self) -> str:
        """返回对象的调试友好字符串表示。

        Returns:
            str: 包含关键字段（trace_id、status）的字符串，用于日志输出和调试。
        """
        return f"<AuditLog(trace_id={self.trace_id}, status={self.status})>"
