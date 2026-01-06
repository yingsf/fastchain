from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Union

import tiktoken
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatGenerationChunk, LLMResult

from ...monitor.metrics import metrics
from ...resources.events import Event, EventBus, EventConstants
from ...tracing.context import get_trace_id


class LLMCallbackHandler(BaseCallbackHandler):
    """专用审计与监控回调处理器

    集成以下功能：
    1. 审计日志：记录每次 LLM 调用的输入、输出、延迟、Token 消耗等
    2. Token 统计：支持服务端返回的 token_usage，也支持本地估算（降级策略）
    3. TTFT 计算：Time To First Token，衡量流式响应的首字延迟
    4. Prometheus 指标：发送请求计数、延迟、Token 消耗等指标

    标准化：
    - 使用 EventConstants 替代硬编码字符串，确保跨模块通信契约一致

    Attributes:
        event_bus (EventBus): 事件总线，用于发布审计事件
        model_alias (str): 模型别名（用于标识和分组）
        scene (Optional[str]): 业务场景标识。默认为 "default"
        extra_metadata (Optional[Dict[str, Any]]): 额外的元数据（用于审计）
        max_audit_length (int): 审计日志文本最大长度，超过截断。默认为 2000
        max_stream_buffer_size (int): 流式缓冲区最大 Token 数，超过丢弃。默认为 10000
    """

    def __init__(
            self,
            event_bus: EventBus,
            model_alias: str,
            scene: Optional[str] = None,
            extra_metadata: Optional[Dict[str, Any]] = None,
            max_audit_length: int = 2000,
            max_stream_buffer_size: int = 10000,
    ) -> None:
        """初始化 LLMCallbackHandler

        Args:
            event_bus (EventBus): 事件总线
            model_alias (str): 模型别名
            scene (Optional[str]): 业务场景标识。默认为 "default"
            extra_metadata (Optional[Dict[str, Any]]): 额外的元数据
            max_audit_length (int): 审计日志文本截断长度。默认为 2000。0 表示不截断。
            max_stream_buffer_size (int): 流式缓冲区最大容量。默认为 10000。0 表示不限制。
        """
        super().__init__()
        self.event_bus = event_bus
        self.model_alias = model_alias
        self.scene = scene or "default"
        self.extra_metadata = extra_metadata or {}

        # 保存限制配置
        self.max_audit_length = max_audit_length
        self.max_stream_buffer_size = max_stream_buffer_size

        # 从分布式追踪上下文中获取 Trace ID
        self._trace_id = get_trace_id() or "no-trace"
        self._start_time: float = 0.0
        self._first_token_time: float | None = None
        self._stream_buffer: List[str] = []
        self._prompt_messages: List[BaseMessage] = []

        # 尝试初始化 tiktoken 编码器（用于精确的 Token 计数），如果失败（如 tiktoken 未安装），则使用降级策略
        try:
            self._tokenizer = tiktoken.get_encoding("cl100k_base")
        except Exception:
            self._tokenizer = None

    def _count_tokens(self, text: str) -> int:
        """估算文本的 Token 数量（降级策略）"""
        if not text:
            return 0

        # 1. 优先使用 Tokenizer（最准确）
        if self._tokenizer:
            try:
                return len(self._tokenizer.encode(text))
            except Exception:
                pass

        # 2. 降级策略：检测是否包含非 ASCII 字符（如中文）
        try:
            text.encode('ascii')
            # 纯英文：假设平均每个单词 4 个字符，1 个单词 = 1 token
            return max(1, len(text) // 4)
        except UnicodeEncodeError:
            # 含中文/Emoji：保守估计 1 char = 1 token
            return len(text)

    def _sanitize_text(self, text: str) -> str:
        """文本脱敏与截断"""
        if not text:
            return ""
        # 0 表示不截断 (Unrestricted)
        if self.max_audit_length <= 0:
            return text

        if len(text) > self.max_audit_length:
            return text[:self.max_audit_length] + "...(truncated)"
        return text

    def _sanitize_messages(self, messages: List[BaseMessage]) -> List[Dict[str, Any]]:
        """消息列表序列化与脱敏"""
        safe_msgs = []
        for m in messages:
            # 获取原始内容
            content = m.content if isinstance(m.content, str) else str(m.content)
            # 构造安全的消息对象
            safe_msgs.append({
                "type": m.type,
                "content": self._sanitize_text(content),
                # 保留其他可能有用的元数据，但不包括大字段
                "additional_kwargs": m.additional_kwargs
            })
        return safe_msgs

    async def on_chat_model_start(
            self,
            serialized: Dict[str, Any],
            messages: List[List[BaseMessage]],
            **kwargs: Any
    ) -> None:
        """LLM 调用开始时的回调"""
        self._start_time = time.perf_counter()
        self._first_token_time = None
        self._stream_buffer = []
        # 记录输入消息（取第一个批次）
        if messages:
            self._prompt_messages = messages[0]

    async def on_llm_new_token(
            self,
            token: str,
            *,
            chunk: Optional[ChatGenerationChunk] = None,
            **kwargs: Any
    ) -> None:
        """流式响应中新 Token 到达时的回调"""
        # 记录首个 Token 的到达时间（只记录一次）
        if self._first_token_time is None:
            self._first_token_time = time.perf_counter()

        # 防止缓冲区无限增长导致 OOM
        if self.max_stream_buffer_size > 0:
            if len(self._stream_buffer) >= self.max_stream_buffer_size:
                return

        # 将 Token 添加到缓冲区，优先使用 chunk.message.content（更可靠），否则使用 token 参数
        if chunk and chunk.message and isinstance(chunk.message.content, str):
            self._stream_buffer.append(chunk.message.content)
        elif token:
            self._stream_buffer.append(token)

    async def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """LLM 调用成功完成时的回调"""
        # 计算总延迟
        end_time = time.perf_counter()
        latency_ms = (end_time - self._start_time) * 1000
        duration_sec = end_time - self._start_time

        # 计算 TTFT（如果是流式调用）
        ttft_ms = None
        if self._first_token_time:
            ttft_ms = (self._first_token_time - self._start_time) * 1000

        # 1. 提取生成的文本内容
        generation_text = ""
        if response.generations and response.generations[0]:
            gen = response.generations[0][0]
            # 兼容两种格式：AIMessage 和纯文本
            generation_text = str(gen.message.content) if isinstance(gen.message, BaseMessage) else gen.text
        if not generation_text and self._stream_buffer:
            # 流式场景下，从缓冲区拼接完整输出
            generation_text = "".join(self._stream_buffer)

        # 2. Token 计算
        llm_output = response.llm_output or {}
        token_usage = llm_output.get("token_usage", {})

        # 如果服务端未返回 token_usage，或返回的数据为空，则本地估算
        if not token_usage or token_usage.get("total_tokens", 0) == 0:
            prompt_text = "".join([m.content for m in self._prompt_messages if isinstance(m.content, str)])
            p = self._count_tokens(prompt_text)
            c = self._count_tokens(generation_text)
            token_usage = {
                "prompt_tokens": p,
                "completion_tokens": c,
                "total_tokens": p + c
            }

        # 3. Prometheus 打点
        metrics.llm_requests_total.labels(
            model=self.model_alias, scene=self.scene, status="success"
        ).inc()

        metrics.llm_request_duration_seconds.labels(
            model=self.model_alias, scene=self.scene
        ).observe(duration_sec)

        if token_usage:
            metrics.llm_token_usage_total.labels(
                model=self.model_alias, scene=self.scene, type="prompt"
            ).inc(token_usage.get("prompt_tokens", 0))
            metrics.llm_token_usage_total.labels(
                model=self.model_alias, scene=self.scene, type="completion"
            ).inc(token_usage.get("completion_tokens", 0))

        # 4. 发送 EventBus 审计事件
        audit_payload = {
            EventConstants.KEY_TRACE_ID: self._trace_id,
            EventConstants.KEY_MODEL_ALIAS: self.model_alias,
            EventConstants.KEY_SCENE: self.scene,
            EventConstants.KEY_LATENCY_MS: round(latency_ms, 2),
            EventConstants.KEY_STATUS: "success",
            EventConstants.KEY_INPUT: self._sanitize_messages(self._prompt_messages),
            EventConstants.KEY_OUTPUT: self._sanitize_text(generation_text),
            EventConstants.KEY_METADATA: self.extra_metadata,
            EventConstants.KEY_TIMESTAMP: time.time(),
            EventConstants.KEY_PROMPT_TOKENS: token_usage.get("prompt_tokens", 0),
            EventConstants.KEY_COMPLETION_TOKENS: token_usage.get("completion_tokens", 0),
            EventConstants.KEY_TOTAL_TOKENS: token_usage.get("total_tokens", 0),
            EventConstants.KEY_TTFT_MS: round(ttft_ms, 2) if ttft_ms else None
        }
        await self.event_bus.publish(
            Event(type=EventConstants.LLM_AUDIT_SUCCESS, payload=audit_payload)
        )

    async def on_llm_error(self, error: Union[Exception, KeyboardInterrupt], **kwargs: Any) -> None:
        """LLM 调用失败时的回调"""
        latency_ms = (time.perf_counter() - self._start_time) * 1000

        metrics.llm_requests_total.labels(
            model=self.model_alias, scene=self.scene, status="error"
        ).inc()

        ttft_ms = None
        if self._first_token_time:
            ttft_ms = (self._first_token_time - self._start_time) * 1000

        # 构造错误审计载荷
        error_payload = {
            EventConstants.KEY_TRACE_ID: self._trace_id,
            EventConstants.KEY_MODEL_ALIAS: self.model_alias,
            EventConstants.KEY_SCENE: self.scene,
            EventConstants.KEY_LATENCY_MS: round(latency_ms, 2),
            EventConstants.KEY_STATUS: "error",
            EventConstants.KEY_ERROR_TYPE: type(error).__name__,
            EventConstants.KEY_ERROR: str(error),
            EventConstants.KEY_INPUT: self._sanitize_messages(self._prompt_messages),
            EventConstants.KEY_METADATA: self.extra_metadata,
            EventConstants.KEY_TTFT_MS: round(ttft_ms, 2) if ttft_ms else None
        }
        await self.event_bus.publish(
            Event(type=EventConstants.LLM_AUDIT_ERROR, payload=error_payload)
        )
