from __future__ import annotations

import json
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Type, TypeVar, Union

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from loguru import logger
from pydantic import BaseModel

from .manager import LLMManager
from ...core.config import SettingsManager
from ...core.llm.base.callback import LLMCallbackHandler
from ...core.llm.prompts.manager import PromptManager
from ...core.llm.types import StreamChunk
from ...core.llm.utils.parsers import OutputParser
from ...core.resources.events import Event, EventBus, EventConstants
from ...core.tracing.context import get_trace_id

T = TypeVar("T", bound=BaseModel)


class ChatService:
    """LLM 统一调用服务

    提供三大核心能力：
    1. 结构化输出（chat_structure）：支持 Self-Healing 的自动纠错机制
    2. 普通对话（chat_invoke）：非流式同步调用
    3. 流式对话（chat_stream）：实时 Token 级响应

    架构特性：
    - 集成审计与监控：所有调用通过 Callback 发送审计事件和 Prometheus 指标
    - 上下文保护：自动检查 Prompt Token 数，防止超出模型上下文窗口
    - 限流控制：与 LLMRuntime 配合，支持并发数和 QPS 限制
    - 模板支持：通过 PromptManager 渲染 Jinja2 模板
    - 配置驱动：支持从 SettingsManager 动态读取安全限制和重试策略

    Attributes:
        self._manager (LLMManager): LLM 运行时管理器，负责模型实例生命周期。
        self._event_bus (EventBus): 事件总线，用于发布审计和监控事件。
        self._prompt_manager (PromptManager): Prompt 模板管理器，支持 Jinja2 渲染。
        self._settings (Optional[SettingsManager]): 全局配置管理器，用于获取动态配置。

    Methods:
        chat_structure: 结构化输出接口（带自愈能力）
        chat_invoke: 普通对话接口（非流式）
        chat_stream: 流式对话接口
    """

    def __init__(
            self,
            manager: LLMManager,
            event_bus: EventBus,
            prompt_manager: PromptManager,
            settings: Optional[SettingsManager] = None
    ) -> None:
        """初始化 ChatService

        Args:
            manager (LLMManager): LLM 运行时管理器
            event_bus (EventBus): 事件总线，用于审计和监控
            prompt_manager (PromptManager): Prompt 模板管理器
            settings (Optional[SettingsManager]): 配置管理器
        """
        self._manager = manager
        self._event_bus = event_bus
        self._prompt_manager = prompt_manager
        self._settings = settings

    def _get_security_limits(self) -> Dict[str, int]:
        """从配置中心读取动态安全限制"""
        # 默认兜底值
        limits = {
            "audit_log_max_length": 2000,
            "stream_buffer_max_tokens": 10000
        }

        if not self._settings:
            return limits

        try:
            ns = self._settings.local.apollo_namespace
            # 安全读取 system.config
            sys_conf = self._settings.get_value(ns, "system.config", default={})

            # 兼容 JSON 字符串或字典格式
            if isinstance(sys_conf, str):
                try:
                    sys_conf = json.loads(sys_conf)
                except Exception:
                    sys_conf = {}

            if isinstance(sys_conf, dict):
                security = sys_conf.get("security", {})
                # 覆盖默认值
                if "audit_log_max_length" in security:
                    limits["audit_log_max_length"] = int(security["audit_log_max_length"])
                if "stream_buffer_max_tokens" in security:
                    limits["stream_buffer_max_tokens"] = int(security["stream_buffer_max_tokens"])

        except Exception as e:
            logger.warning(f"从设置中加载安全限制失败: {e}")

        return limits

    def _get_retry_config(self) -> int:
        """从配置中心读取结构化重试次数"""
        default_retries = 3
        if not self._settings:
            return default_retries

        try:
            ns = self._settings.local.apollo_namespace
            sys_conf = self._settings.get_value(ns, "system.config", default={})

            if isinstance(sys_conf, str):
                try:
                    sys_conf = json.loads(sys_conf)
                except Exception:
                    sys_conf = {}

            if isinstance(sys_conf, dict):
                llm_service = sys_conf.get("llm_service", {})
                return int(llm_service.get("structure_retry_count", default_retries))
        except Exception:
            pass

        return default_retries

    async def chat_structure(
            self,
            response_model: Type[T],
            template_id: str,
            variables: Optional[Dict[str, Any]] = None,
            alias: Optional[str] = None,
            scene: str = "default",
            max_retries: Optional[int] = None,
            **kwargs: Any
    ) -> T:
        """结构化输出接口"""
        start_time = time.perf_counter()
        variables = variables or {}
        input_audit = {"template_id": template_id, "variables": variables, "target_schema": response_model.__name__}

        # 动态读取重试次数
        if max_retries is None:
            max_retries = self._get_retry_config()

        try:
            runtime = self._resolve_runtime(alias)
            format_instructions = OutputParser.get_format_instructions(response_model)
            base_messages = self._prompt_manager.render_messages(template_id, variables)
            base_messages_with_schema = self._inject_format_instructions(base_messages, format_instructions)

            last_error = None

            for attempt in range(max_retries + 1):
                metadata = kwargs.get("metadata", {})
                metadata.update({
                    "template_id": template_id,
                    "variables": variables,
                    "structure_attempt": attempt + 1
                })

                # 每次重试使用干净的消息副本，防止历史污染
                messages_to_send = list(base_messages_with_schema)

                # 如果是重试且有错误，临时追加"纠错提示"
                if attempt > 0 and last_error:
                    retry_instruction = (
                        f"\n\n[System Notification]\n"
                        f"Your previous output failed to parse. Error: {str(last_error)}\n"
                        f"Please output valid JSON only, based on the schema."
                    )
                    messages_to_send.append(HumanMessage(content=retry_instruction))

                # 调用 LLM
                response_content = await self._internal_invoke(
                    runtime, messages_to_send, scene, metadata, **kwargs
                )

                try:
                    # 尝试解析
                    parsed_obj = OutputParser.parse(response_content, response_model)
                    return parsed_obj
                except Exception as e:
                    # 解析失败，记录错误并继续下一次循环
                    last_error = e
                    logger.warning(f"结构化解析失败 (尝试 {attempt + 1}/{max_retries}): {e}")

            # 如果所有重试都失败，抛出最终错误
            raise ValueError(f"在 {max_retries} 次重试后仍无法解析结构化输出。最后一次错误: {last_error}")

        except Exception as e:
            # 系统级错误（非解析错误），发送审计事件
            await self._emit_system_error(
                e, alias, scene, start_time, input_audit, kwargs.get("metadata")
            )
            raise e

    async def chat_invoke(
            self,
            messages: Union[str, List[BaseMessage]] = None,
            alias: Optional[str] = None,
            scene: str = "default",
            template_id: Optional[str] = None,
            variables: Optional[Dict[str, Any]] = None,
            **kwargs: Any
    ) -> str:
        """非流式对话接口"""
        start_time = time.perf_counter()
        if template_id:
            input_audit = {"template_id": template_id, "variables": variables}
        else:
            input_audit = {"messages": str(messages)}

        try:
            # 解析模型别名，获取对应的 Runtime
            runtime = self._resolve_runtime(alias)
            # 准备输入消息和元数据
            input_msgs, final_metadata = self._prepare_input(
                messages, template_id, variables, kwargs.get("metadata")
            )

            # 复用底层调用逻辑
            return await self._internal_invoke(runtime, input_msgs, scene, final_metadata, **kwargs)

        except Exception as e:
            # 发送系统级错误事件
            await self._emit_system_error(
                e, alias, scene, start_time, input_audit, kwargs.get("metadata")
            )
            raise e

    async def chat_stream(
            self,
            messages: Union[str, List[BaseMessage]] = None,
            alias: Optional[str] = None,
            scene: str = "default",
            template_id: Optional[str] = None,
            variables: Optional[Dict[str, Any]] = None,
            **kwargs: Any
    ) -> AsyncIterator[StreamChunk]:
        """流式对话接口"""
        start_time = time.perf_counter()
        if template_id:
            input_audit = {"template_id": template_id, "variables": variables}
        else:
            input_audit = {"messages": str(messages)}

        try:
            # 在生成器启动前完成所有同步准备工作
            runtime = self._resolve_runtime(alias)
            input_msgs, final_metadata = self._prepare_input(
                messages, template_id, variables, kwargs.get("metadata")
            )
            # 上下文长度检查
            self._check_context_length(runtime, input_msgs)

        except Exception as e:
            # 在生成器启动前的错误，发送审计事件后直接抛出
            await self._emit_system_error(
                e, alias, scene, start_time, input_audit, kwargs.get("metadata")
            )
            raise e

        # 读取动态限制，传递给 Callback
        limits = self._get_security_limits()

        callback = LLMCallbackHandler(
            event_bus=self._event_bus,
            model_alias=runtime.alias,
            scene=scene,
            extra_metadata=final_metadata,
            max_audit_length=limits["audit_log_max_length"],
            max_stream_buffer_size=limits["stream_buffer_max_tokens"]
        )

        try:
            # 使用提取的方法执行流式逻辑
            async for chunk in self._execute_stream_with_limiter(
                    runtime, input_msgs, callback, **kwargs
            ):
                yield chunk

            # 流结束标记
            yield StreamChunk(type="end", content=None, error=None, metadata={})

        except Exception as e:
            # 流式传输过程中的错误，通过 StreamChunk 返回
            logger.error(f"模型 '{runtime.alias}' 流式传输错误: {e}")
            yield StreamChunk(
                type="error",
                content=None,
                error=str(e),
                metadata={}
            )

    async def _execute_stream_with_limiter(
            self,
            runtime: Any,
            input_msgs: List[BaseMessage],
            callback: LLMCallbackHandler,
            **kwargs: Any
    ) -> AsyncIterator[StreamChunk]:
        """在限流器上下文中执行流式生成"""
        # 创建底层生成器
        stream_gen = self._create_stream_generator(runtime, input_msgs, callback, **kwargs)

        if runtime.limiter:
            async with runtime.limiter.slot():
                async for item in stream_gen:
                    yield item
        else:
            async for item in stream_gen:
                yield item

    async def _create_stream_generator(
            self,
            runtime: Any,
            input_msgs: List[BaseMessage],
            callback: LLMCallbackHandler,
            **kwargs: Any
    ) -> AsyncIterator[StreamChunk]:
        """创建原始模型流式生成器"""
        async for chunk in runtime.model.astream(
                input_msgs,
                config={"callbacks": [callback]},
                **kwargs
        ):
            content = ""
            if hasattr(chunk, "content"):
                content = str(chunk.content)
            elif hasattr(chunk, "message") and hasattr(chunk.message, "content"):
                content = str(chunk.message.content)

            if content:
                yield StreamChunk(
                    type="token",
                    content=content,
                    error=None,
                    metadata={}
                )

    async def _internal_invoke(
            self, runtime: Any, messages: List[BaseMessage], scene: str, metadata: Dict, **kwargs
    ) -> str:
        """底层调用实现：包含上下文检查、回调挂载、限流控制"""
        self._check_context_length(runtime, messages)

        # 读取动态限制，并传给 Callback
        limits = self._get_security_limits()

        callback = LLMCallbackHandler(
            event_bus=self._event_bus,
            model_alias=runtime.alias,
            scene=scene,
            extra_metadata=metadata,
            max_audit_length=limits["audit_log_max_length"],
            max_stream_buffer_size=limits["stream_buffer_max_tokens"]
        )

        config = {"callbacks": [callback]}

        # 应用限流器（如果配置了的话）
        if runtime.limiter:
            async with runtime.limiter.slot():
                resp = await runtime.model.ainvoke(messages, config=config, **kwargs)
        else:
            resp = await runtime.model.ainvoke(messages, config=config, **kwargs)

        return str(resp.content)

    def _check_context_length(self, runtime: Any, messages: List[BaseMessage]) -> None:
        """上下文长度熔断保护"""
        if not runtime.config.max_context_length:
            return

        current_tokens = runtime.estimate_tokens(messages)
        reserved_output = runtime.config.max_tokens
        total_estimated = current_tokens + reserved_output
        limit = runtime.config.max_context_length

        if total_estimated > limit:
            raise ValueError(
                f"上下文长度超限！预估总计: {total_estimated} (提示词: {current_tokens} + 输出: {reserved_output}), "
                f"限制: {limit}。请缩短您的提示词。"
            )

    def _inject_format_instructions(self, messages: List[BaseMessage], instructions: str) -> List[BaseMessage]:
        """将 JSON Schema 指令注入到消息列表中（Ensure SystemMessage）"""
        new_msgs = list(messages)
        # 优先追加到已有的 System Message
        for i, msg in enumerate(new_msgs):
            if isinstance(msg, SystemMessage):
                new_content = f"{msg.content}\n\n{instructions}"
                new_msgs[i] = SystemMessage(content=new_content)
                return new_msgs

        # 如果没有 System Message，则在头部插入一个
        new_msgs.insert(0, SystemMessage(content=f"System Instruction:\n{instructions}"))
        return new_msgs

    async def _emit_system_error(
            self,
            error: Exception,
            alias: Optional[str],
            scene: str,
            start_time: float,
            input_info: Dict[str, Any],
            metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """发送系统级错误审计（在 Callback 无法接管时使用）"""
        latency_ms = (time.perf_counter() - start_time) * 1000

        payload = {
            EventConstants.KEY_TRACE_ID: get_trace_id() or "no-trace",
            EventConstants.KEY_MODEL_ALIAS: alias or "unknown",
            EventConstants.KEY_SCENE: scene,
            EventConstants.KEY_STATUS: "error",
            EventConstants.KEY_LATENCY_MS: round(latency_ms, 2),
            EventConstants.KEY_ERROR_TYPE: type(error).__name__,
            EventConstants.KEY_ERROR: str(error),
            EventConstants.KEY_INPUT: input_info,
            EventConstants.KEY_METADATA: metadata or {},
            EventConstants.KEY_OUTPUT: None
        }
        await self._event_bus.publish(
            Event(type=EventConstants.LLM_AUDIT_ERROR, payload=payload)
        )

    def _resolve_runtime(self, alias: Optional[str]) -> Any:
        """解析模型别名，获取对应的 LLMRuntime"""
        if alias:
            runtime = self._manager.get_runtime(alias)
            if not runtime:
                raise ValueError(f"在 LLMManager 中未找到模型别名 '{alias}'。")
        else:
            runtime = self._manager.get_default_runtime()
            if not runtime:
                raise ValueError("LLMManager 中未配置默认模型。")

        if not runtime.is_active:
            raise RuntimeError(f"模型 '{runtime.alias}' 处于非活跃状态。")

        return runtime

    def _prepare_input(
            self,
            messages: Union[str, List[BaseMessage], None],
            template_id: Optional[str],
            variables: Optional[Dict[str, Any]],
            existing_metadata: Optional[Dict[str, Any]]
    ) -> tuple[List[BaseMessage], Dict[str, Any]]:
        """准备输入消息和元数据"""
        metadata = existing_metadata or {}

        if template_id:
            # 模板模式：通过 PromptManager 渲染
            variables = variables or {}
            input_msgs = self._prompt_manager.render_messages(template_id, variables)
            metadata.update({"template_id": template_id, "variables": variables})
        elif messages:
            # 直接消息模式：标准化为 BaseMessage 列表
            input_msgs = self._normalize_messages(messages)
        else:
            raise ValueError("必须提供 'messages' 或 'template_id' 之一。")

        return input_msgs, metadata

    def _normalize_messages(self, messages: Union[str, List[BaseMessage]]) -> List[BaseMessage]:
        """标准化消息格式"""
        if isinstance(messages, str):
            return [HumanMessage(content=messages)]
        return messages
