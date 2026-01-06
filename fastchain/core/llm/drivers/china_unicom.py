from __future__ import annotations

import hashlib
import inspect
import json
import random
import string
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import httpx
from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from loguru import logger
from pydantic import Field, SecretStr, PrivateAttr


class ChinaUnicomChatModel(BaseChatModel):
    """中国联通内部大模型的 LangChain 适配器

    将中国联通提供的专有大模型 API 封装为符合 LangChain 标准的 ChatModel，支持同步/异步调用、流式输出、自定义认证等企业级特性

    核心设计特点：
        - 协议适配：将联通专有协议转换为 LangChain 标准接口
        - 安全认证：支持双重认证（APP_ID/SECRET + Bearer Token）
        - 流式优化：实现高性能异步流式输出，降低首字延迟
        - 容错处理：对非标准响应格式进行兼容性处理

    认证机制：
        1. Token 签名：使用 APP_ID/SECRET 生成 MD5 签名，防止重放攻击
        2. Bearer Token：API Key 认证，支持网关鉴权。
        3. 场景码隔离：通过 scene_code 实现多租户隔离

    数据流转：
        输入 (LangChain Messages) → 协议转换 → HTTP 请求 → 响应解析 → 输出 (ChatResult)

    Attributes:
        api_url (str): 联通大模型 API 的 HTTP 端点（通常为企业内网地址）。
        app_id (str): 应用标识符，用于生成认证 Token。
        app_secret (SecretStr): 应用密钥，用于 MD5 签名（敏感信息，自动加密）。
        scene_code (str): 业务场景码，用于多租户隔离和流量标记。
        timeout (float): HTTP 请求超时时间（秒），默认 60 秒。
        default_headers (Dict[str, str]): 自定义 HTTP 头，用于注入额外元数据。
        model_name (str): 模型标识符（如 "gpt-3.5-turbo"），传递给后端选择模型版本。
        temperature (float): 采样温度（0-2），控制输出随机性，默认 0.7。
        max_tokens (int): 最大生成 Token 数，默认 8192（防止超长输出）。
        top_p (float): 核采样阈值（0-1），控制输出多样性，默认 1.0。
        api_key (Optional[SecretStr]): 可选的 Bearer Token，用于网关级认证。
        model_kwargs (Dict[str, Any]): 透传给后端的额外参数（如 frequency_penalty）。
        _client (Optional[httpx.AsyncClient]): 可选的外部 HTTP 客户端（用于连接池复用）。

    Methods:
        _generate: 同步调用（阻塞式）。
        _agenerate: 异步调用（非阻塞）。
        _astream: 异步流式调用（实时输出）。
        _build_payload: 构造符合联通协议的请求体。
        _build_headers: 构造 HTTP 请求头（含认证信息）。
        _process_response: 解析 HTTP 响应为 LangChain 标准格式。
        _generate_token: 生成 MD5 签名（防篡改）。

    Note:
        - 此模型仅适用于中国联通内部网络环境，外网无法访问
        - 联通协议使用三段式结构（HEAD/BODY/ATTACHED），需严格遵守
        - 流式响应采用 SSE（Server-Sent Events）格式，以 "data: [DONE]" 结束

    Warning:
        - app_secret 和 api_key 使用 SecretStr 存储，避免日志泄露
        - 默认 timeout 较长（60 秒），长文本生成时可能需要调整
        - 流式响应可能出现非标准 JSON，已实现容错处理
    """
    api_url: str
    app_id: str
    app_secret: SecretStr
    scene_code: str
    timeout: float = 60.0
    default_headers: Dict[str, str] = Field(default_factory=dict)
    model_name: str = Field(alias="model")
    temperature: float
    max_tokens: int
    top_p: float
    api_key: Optional[SecretStr] = None
    # 透传参数（如 frequency_penalty）
    model_kwargs: Dict[str, Any] = Field(default_factory=dict)

    # 私有属性：外部注入的 HTTP 客户端（用于连接池复用），PrivateAttr 表示此属性不会被序列化到配置文件中
    _client: Optional[httpx.AsyncClient] = PrivateAttr(default=None)

    class Config:
        """Pydantic 配置类"""
        # 允许使用字段别名（如 model 和 model_name 互通）
        populate_by_name = True

    def __init__(self, client: Optional[httpx.AsyncClient] = None, **kwargs):
        """初始化联通大模型适配器

        Args:
            client (Optional[httpx.AsyncClient]): 可选的外部 HTTP 客户端。
                如果提供，将复用该客户端的连接池，提升性能。
                如果未提供，每次调用时会创建临时客户端。
            **kwargs: 其他字段（如 api_url, app_id 等），传递给 Pydantic。

        Note:
            依赖注入设计：
            - 外部传入 client 可以实现连接池共享，减少 TCP 握手开销。
            - 未传入时，使用临时客户端，适合低频调用场景。
        """
        # 调用 BaseChatModel 的初始化逻辑
        super().__init__(**kwargs)
        # 注入外部客户端（可选）
        self._client = client

    @property
    def _llm_type(self) -> str:
        """返回 LLM 类型标识符（LangChain 内部使用）

        Returns:
            str: 固定返回 "china-unicom-llm"，用于日志和监控

        Note:
            此方法是 LangChain 的约定接口，用于区分不同的 LLM 实现
        """
        return "china-unicom-llm"

    def _get_timestamp(self) -> str:
        """生成当前时间戳（联通协议要求的格式）

        Returns:
            str: 格式为 "YYYY-MM-DD HH:MM:SS.mmm" 的时间戳（毫秒精度）
        """
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")[:-3]

    def _get_trans_id(self) -> str:
        """生成唯一事务 ID（联通协议要求）

        Returns:
            str: 格式为 "YYYYMMDDHHMMSSmmm + 6 位随机数" 的事务 ID
        """
        # 生成时间戳部分（17 位）
        timestamp_part = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]

        # 生成 6 位随机数字
        random_part = "".join(random.choices(string.digits, k=6))

        return timestamp_part + random_part

    def _generate_token(self, params: dict) -> str:
        """生成 MD5 签名 Token（联通安全认证机制）

        Returns:
            str: 32 位 MD5 哈希值（小写十六进制）
        """
        # 1. 过滤掉 TOKEN 字段（避免循环依赖），按 key 升序排序，使用大写比较（k.upper()）确保忽略大小写差异
        sorted_items = sorted((k, v) for k, v in params.items() if k.upper() != "TOKEN")

        # 2. 拼接为 "key1value1key2value2..." 格式
        base_str = "".join(f"{k}{v}" for k, v in sorted_items)

        # 3. 追加密钥（从 SecretStr 中提取明文，并去除首尾空格）
        base_str += self.app_secret.get_secret_value().strip()

        # 4. 计算 MD5 哈希值（UTF-8 编码，返回 32 位十六进制字符串）
        return hashlib.md5(base_str.encode("utf-8")).hexdigest()

    def _build_headers(self) -> Dict[str, str]:
        """构造 HTTP 请求头（含认证信息）

        Returns:
            Dict[str, str]: 完整的 HTTP 请求头字典。
        """
        # 1. 设置基础请求头
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json",
            "Accept-Encoding": "",
            "scene_code": self.scene_code,
        }

        # 2. 处理 API Key 认证（可选）
        if self.api_key:
            # 从 SecretStr 中提取明文，并去除首尾空格
            raw_key = self.api_key.get_secret_value().strip()

            if raw_key:
                # 判断是否已包含 "Bearer " 前缀（不区分大小写）
                if raw_key.lower().startswith("bearer "):
                    # 直接使用原始 Token（保留大小写）
                    headers["nlpt-Authorization"] = raw_key
                else:
                    # 自动添加 "Bearer " 前缀
                    headers["nlpt-Authorization"] = f"Bearer {raw_key}"
            else:
                # API Key 为空字符串，设置为空（后端兼容处理）
                headers["nlpt-Authorization"] = ""
        else:
            # 未提供 API Key，设置为空字符串
            headers["nlpt-Authorization"] = ""

        # 3. 合并用户自定义请求头（可能覆盖默认值）
        headers.update(self.default_headers)

        return headers

    def _build_payload(
            self, messages: List[BaseMessage], stream: bool = False, **kwargs: Any
    ) -> Dict[str, Any]:
        """构造符合联通协议的请求体（三段式结构）

        Args:
            messages (List[BaseMessage]): LangChain 标准消息列表
            stream (bool): 是否启用流式输出，默认 False
            **kwargs: 额外参数，会覆盖默认值（如 temperature）

        Returns:
            Dict[str, Any]: 符合联通协议的请求体字典
        """
        # ==================== 1. 消息格式转换 ====================
        formatted_msgs = []
        for m in messages:
            # 默认角色为 "assistant"（防御式编程）
            role = "assistant"

            # 根据 LangChain 消息类型转换角色
            if m.type == "human":
                # 用户消息
                role = "user"
            elif m.type == "system":
                # 系统提示词
                role = "system"
            # 注意：m.type == "ai" 时，role 保持为 "assistant"

            # 添加到格式化列表
            formatted_msgs.append({"role": role, "content": m.content})

        # ==================== 2. 构造业务参数 ====================
        base_params = {
            "model": self.model_name,
            "messages": formatted_msgs,
            "stream": stream,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p
        }

        # 3. 合并配置参数（model_kwargs），过滤 None 值
        if self.model_kwargs:
            base_params.update(
                {
                    k: v for k, v in self.model_kwargs.items() if v is not None
                }
            )

        # 4. 合并动态参数（kwargs），优先级最高
        base_params.update(
            {
                k: v for k, v in kwargs.items() if v is not None
            }
        )

        # ==================== 5. 生成认证参数 ====================
        sys_params = {
            "APP_ID": self.app_id,
            "TIMESTAMP": self._get_timestamp(),
            "TRANS_ID": self._get_trans_id(),
        }

        # 生成 MD5 签名 Token（基于 sys_params 和 app_secret）
        token = self._generate_token(sys_params)
        sys_params["TOKEN"] = token

        # ==================== 6. 组装三段式请求体 ====================
        return {
            "UNI_BSS_HEAD": sys_params,
            "UNI_BSS_BODY": {
                "YUANJING_MODEL_REQ": base_params,
            },
            "UNI_BSS_ATTACHED": {"MEDIA_INFO": ""}
        }

    def _process_response(self, response: httpx.Response) -> ChatResult:
        """解析 HTTP 响应为 LangChain 标准格式（非流式）

        Args:
            response (httpx.Response): HTTP 响应对象。

        Returns:
            ChatResult: LangChain 标准的生成结果对象。

        Raises:
            ValueError: 当 HTTP 状态码非 200 时抛出。
        """
        # 1. 检查 HTTP 状态码（非 200 直接抛出异常）
        if response.status_code != 200:
            raise ValueError(f"HTTP {response.status_code}: {response.text}")

        # 2. 调用 httpx 的内置方法，抛出 4xx/5xx 异常
        response.raise_for_status()

        # 3. 解析 JSON 响应体
        data = response.json()

        # 4. 提取生成内容（兼容多种格式）
        content = self._extract_content(data)

        # 5. 封装为 ChatResult 对象
        return ChatResult(
            generations=[
                ChatGeneration(message=AIMessage(content=content))
            ]
        )

    def _extract_content(self, data: Dict[str, Any]) -> str:
        """从响应 JSON 中提取生成内容（兼容多种格式）

        Args:
            data (Dict[str, Any]): 响应 JSON 数据

        Returns:
            str: 提取到的生成内容，失败时返回空字符串
        """
        # 1. 尝试提取 OpenAI 兼容格式（choices[0].message.content）
        if "choices" in data and len(data["choices"]) > 0:
            # 优先提取 message.content（标准格式）
            delta = data["choices"][0].get("message", {}).get("content", "")

            # 如果为空，尝试提取 delta.content（流式格式偶现）
            if not delta:
                delta = data["choices"][0].get("delta", {}).get("content", "")

            return delta

        # 2. 尝试提取简化格式（text 字段）
        elif "text" in data:
            return data.get("text", "")

        # 3. 所有路径都失败，返回空字符串
        return ""

    # ==================== LangChain 标准接口实现 ====================

    def _generate(
            self,
            messages: List[BaseMessage],
            stop: Optional[List[str]] = None,
            run_manager: Optional[CallbackManagerForLLMRun] = None,
            **kwargs: Any,
    ) -> ChatResult:
        """同步调用接口（阻塞式）

        Args:
            messages (List[BaseMessage]): LangChain 标准消息列表
            stop (Optional[List[str]]): 停止词列表（当前未使用）
            run_manager (Optional[CallbackManagerForLLMRun]): 回调管理器（用于监控）
            **kwargs: 额外参数（会传递给 _build_payload）

        Returns:
            ChatResult: LangChain 标准的生成结果对象

        Raises:
            ValueError: 当 HTTP 请求失败或响应格式错误时抛出
        """
        # 1. 构造请求体（stream=False，非流式模式）
        payload = self._build_payload(messages, stream=False, **kwargs)

        # 2. 构造请求头
        headers = self._build_headers()

        # 3. 使用同步客户端发送 POST 请求
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(self.api_url, json=payload, headers=headers)
            # 4. 解析响应并返回
            return self._process_response(resp)

    async def _agenerate(
            self,
            messages: List[BaseMessage],
            stop: Optional[List[str]] = None,
            run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
            **kwargs: Any,
    ) -> ChatResult:
        """异步调用接口（非阻塞）

        执行以下操作：
            1. 构造请求体（非流式模式）
            2. 构造请求头（含认证信息）
            3. 发送 HTTP POST 请求（使用异步客户端）
            4. 解析响应并返回结果

        Args:
            messages (List[BaseMessage]): LangChain 标准消息列表
            stop (Optional[List[str]]): 停止词列表（当前未使用）
            run_manager (Optional[AsyncCallbackManagerForLLMRun]): 异步回调管理器
            **kwargs: 额外参数（会传递给 _build_payload）

        Returns:
            ChatResult: LangChain 标准的生成结果对象。

        Raises:
            ValueError: 当 HTTP 请求失败或响应格式错误时抛出。
        """
        # 1. 构造请求体（stream=False，非流式模式）
        payload = self._build_payload(messages, stream=False, **kwargs)

        # 2. 构造请求头
        headers = self._build_headers()

        # 3. 使用异步客户端发送 POST 请求
        if self._client:
            # 优先使用外部注入的客户端（连接池复用）
            resp = await self._client.post(self.api_url, json=payload, headers=headers)
            return self._process_response(resp)
        else:
            # 创建临时客户端（自动关闭）
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(self.api_url, json=payload, headers=headers)
                return self._process_response(resp)

    async def _astream(
            self,
            messages: List[BaseMessage],
            stop: Optional[List[str]] = None,
            run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
            **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """异步流式调用接口（实时输出，降低首字延迟）

        Args:
            messages (List[BaseMessage]): LangChain 标准消息列表
            stop (Optional[List[str]]): 停止词列表（当前未使用）
            run_manager (Optional[AsyncCallbackManagerForLLMRun]): 异步回调管理器
            **kwargs: 额外参数（会传递给 _build_payload）

        Yields:
            ChatGenerationChunk: 增量生成结果（每个 chunk 包含部分文本）
        """
        # 1. 构造请求体（stream=True，启用流式输出）
        payload = self._build_payload(messages, stream=True, **kwargs)

        # 2. 构造请求头
        headers = self._build_headers()

        # 3. 选择 HTTP 客户端（优先使用外部注入的客户端）
        if self._client:
            # 使用外部客户端（连接池复用）
            async for chunk in self._execute_stream(self._client, payload, headers, run_manager):
                yield chunk
        else:
            # 创建临时客户端（自动关闭）
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async for chunk in self._execute_stream(client, payload, headers, run_manager):
                    yield chunk

    async def _execute_stream(
            self,
            client: httpx.AsyncClient,
            payload: Dict[str, Any],
            headers: Dict[str, str],
            run_manager: Optional[AsyncCallbackManagerForLLMRun]
    ) -> AsyncIterator[ChatGenerationChunk]:
        """执行流式 HTTP 请求并迭代响应行

        Args:
            client (httpx.AsyncClient): HTTP 客户端实例
            payload (Dict[str, Any]): 请求体（已构造完成）
            headers (Dict[str, str]): 请求头（已构造完成）
            run_manager (Optional[AsyncCallbackManagerForLLMRun]): 异步回调管理器

        Yields:
            ChatGenerationChunk: 增量生成结果。
        """
        # 1. 发送流式 POST 请求（使用 async with 确保连接自动关闭）
        async with client.stream("POST", self.api_url, json=payload, headers=headers) as response:
            # 2. 校验响应状态码（非 200 抛出异常）
            await self._validate_response(response)

            # 3. 逐行迭代 SSE 响应
            async for line in response.aiter_lines():
                # 跳过空行（SSE 协议允许空行作为心跳）
                if not line:
                    continue

                # 检测结束标记（联通协议约定）
                if "data: [DONE]" in line:
                    break  # 提前退出，无需等待连接关闭

                # 4. 处理单行数据（解析 JSON、提取内容、触发回调）
                chunk = await self._process_stream_line(line, run_manager)

                # 如果成功生成 chunk，则 yield 给调用方
                if chunk:
                    yield chunk

    async def _validate_response(self, response: httpx.Response) -> None:
        """校验 HTTP 响应状态码

        Args:
            response (httpx.Response): HTTP 响应对象（流式）

        Raises:
            ValueError: 当 HTTP 状态码非 200 时抛出
        """
        # 1. 检查状态码是否为 200
        if response.status_code != 200:
            # 2. 读取完整响应体（错误信息）
            error_body = await response.aread()

            # 3. 解码为字符串（UTF-8，忽略无效字符）
            error_msg = f"HTTP Error {response.status_code}: {error_body.decode('utf-8', errors='ignore')}"

            # 4. 记录错误日志
            logger.error(error_msg)

            # 5. 抛出异常（中断流式处理）
            raise ValueError(error_msg)

        # 6. 调用 httpx 内置方法，检查 4xx/5xx 错误
        response.raise_for_status()

    async def _process_stream_line(
            self,
            line: str,
            run_manager: Optional[AsyncCallbackManagerForLLMRun]
    ) -> Optional[ChatGenerationChunk]:
        """处理单行 SSE 数据

        Args:
            line (str): SSE 响应行（格式为 "data: {...}"）
            run_manager (Optional[AsyncCallbackManagerForLLMRun]): 异步回调管理器

        Returns:
            Optional[ChatGenerationChunk]: 如果成功提取内容，返回 chunk，否则返回 None
        """
        # 1. 清洗 SSE 格式（移除 "data:" 前缀，去除首尾空格）
        clean_line = line.replace("data:", "").strip()

        # 2. 跳过空行（清洗后可能为空）
        if not clean_line:
            return None

        try:
            # 3. 解析 JSON 数据
            data = json.loads(clean_line)

            # 4. 提取增量内容（delta.content）
            delta = self._extract_delta_content(data)

            # 5. 如果内容非空，生成 chunk 并触发回调
            if delta:
                # 构造 ChatGenerationChunk 对象（使用 model_construct 避免验证开销）
                gen_chunk = ChatGenerationChunk(
                    message=AIMessageChunk.model_construct(content=delta)
                )

                # 触发回调（如果提供了 run_manager）
                if run_manager:
                    # 调用 on_llm_new_token（可能是同步或异步）
                    result = run_manager.on_llm_new_token(delta, chunk=gen_chunk)

                    # 判断是否为异步方法，如果是则 await
                    if inspect.isawaitable(result):
                        await result

                # 返回 chunk 给调用方
                return gen_chunk

        except json.JSONDecodeError:
            # 6. 容错处理：静默捕获 JSON 解析错误（避免流中断）
            pass

        # 7. 解析失败或内容为空，返回 None
        return None

    def _extract_delta_content(self, data: Dict[str, Any]) -> str:
        """从流式 JSON 中提取增量内容

        Args:
            data (Dict[str, Any]): 流式响应的 JSON 数据

        Returns:
            str: 提取到的增量内容，失败时返回空字符串
        """
        # 1. 尝试提取标准流式格式（choices[0].delta.content）
        if "choices" in data and len(data["choices"]) > 0:
            return data["choices"][0].get("delta", {}).get("content", "")

        # 2. 尝试提取简化格式（text 字段）
        elif "text" in data:
            return data.get("text", "")

        # 3. 所有路径都失败，返回空字符串
        return ""
