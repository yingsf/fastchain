from __future__ import annotations

import json
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator


class OpenAIModelConfig(BaseModel):
    """通用大模型配置 Schema

    支持多种 LLM 驱动（OpenAI、ChinaUnicom 等），通过 provider_type 字段区分

    配置分层：
    1. 驱动选择（provider_type）
    2. 身份与连接（api_key、base_url 等）
    3. 核心生成参数（temperature、max_tokens 等）
    4. 差异化参数（model_kwargs，用于透传各驱动的特有参数）
    5. 韧性与流控（timeout、max_retries、max_concurrency 等）
    6. HTTP 定制（default_headers）

    Attributes:
        provider_type (Literal["openai", "china_unicom"]): 底层驱动类型。默认为 "openai"
        model_name (str): 模型 ID（例如 "deepseek"）
        api_key (Optional[SecretStr]): API Key（使用 SecretStr 确保安全性）
        base_url (Optional[str]): API Base URL（用于兼容第三方服务）
        app_id (Optional[str]): 应用 ID（ChinaUnicom 特有）。
        app_secret (Optional[SecretStr]): 应用密钥（ChinaUnicom 特有，用于签名）
        scene_code (Optional[str]): 场景代码（ChinaUnicom 特有）
        organization (Optional[str]): 组织 ID（OpenAI 企业账户使用）
        temperature (float): 采样温度。范围 [0.0, 2.0]，默认为 0.7
        max_tokens (Optional[int]): 最大生成 Token 数。
        top_p (Optional[float]): Nucleus sampling 参数。范围 [0.0, 1.0]，默认为 1.0
        model_kwargs (Dict[str, Any]): 透传给底层 API 的额外参数（如 presence_penalty）
        timeout (float): 请求超时时间（秒）。默认为 60.0
        max_retries (int): 最大重试次数。默认为 1
        max_concurrency (int): 最大并发数。默认为 10
        qps (Optional[float]): QPS 限制（每秒请求数）
        max_context_length (Optional[int]): 最大上下文长度（Prompt + Completion 的总 Token 数）
        default_headers (Dict[str, str]): 自定义 HTTP Headers
    """

    # 1. 驱动选择
    provider_type: Literal["openai", "china_unicom"] = Field(
        default="openai",
        description="底层驱动类型"
    )

    # 2. 身份与连接
    model_name: str = Field(..., description="Model ID")
    api_key: Optional[SecretStr] = Field(default=None, description="API Key")
    base_url: Optional[str] = Field(default=None, description="API Base URL")

    # ChinaUnicom 特有字段
    app_id: Optional[str] = Field(default=None)
    app_secret: Optional[SecretStr] = Field(default=None)
    scene_code: Optional[str] = Field(default=None)
    organization: Optional[str] = None

    # 3. 核心生成参数
    temperature: float = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(default=None, description="Max completion tokens")
    top_p: Optional[float] = Field(default=1.0, ge=0.0, le=1.0, description="Nucleus sampling")

    # 4. 差异化参数：例如 presence_penalty, frequency_penalty, seed 等进这里
    model_kwargs: Dict[str, Any] = Field(default_factory=dict)

    # 5. 韧性与流控
    timeout: float = Field(default=60.0, ge=1.0)
    max_retries: int = Field(default=1, ge=0)
    max_concurrency: int = Field(default=10, ge=1)
    qps: Optional[float] = Field(default=None)

    # 上下文长度保护 (Input + Output)，比如: DeepSeek 是 32k, Yuanjing 是 8k，用于 ChatService._check_context_length 熔断保护
    max_context_length: Optional[int] = Field(default=None, description="Max total tokens (prompt + completion)")

    # 6. HTTP 定制
    default_headers: Dict[str, str] = Field(default_factory=dict)

    class Config:
        # 忽略未知字段，防止配置更新时因新增字段导致解析失败
        extra = "ignore"

    @field_validator("model_kwargs", "default_headers", mode="before")
    @classmethod
    def _parse_json_string(cls, v: Any) -> Any:
        """智能解析 JSON 字符串

        处理 Apollo 配置中常见的字符串格式：
        - 空字符串 -> 空字典
        - JSON 字符串 -> 解析为字典
        - 已经是字典 -> 直接返回

        这种设计可以兼容不同的配置来源（Apollo、环境变量等）

        Args:
            v: 原始值（可能是字符串或字典）

        Returns:
            Dict: 解析后的字典

        Raises:
            ValueError: 当 JSON 格式错误时
        """
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return {}
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"无效的 JSON 字符串: {e}") from e
        return v

    @field_validator("api_key", "app_secret", mode="before")
    @classmethod
    def _parse_secret(cls, v: Any) -> Any:
        """处理空字符串的 SecretStr 字段

        防御性设计：将空字符串转换为 None，避免 SecretStr("") 导致的语义混淆

        Args:
            v: 原始值（可能是空字符串或有效的密钥）

        Returns:
            Any: None（如果是空字符串）或原始值
        """
        if v == "":
            return None
        return v


class LLMConfig(BaseModel):
    """LLM 配置容器（对应 Apollo 中的 llm.models Key）

    包含多个模型配置和默认模型指定

    Attributes:
        models (Dict[str, OpenAIModelConfig]): 模型别名 -> 配置的映射
        default_model (Optional[str]): 默认模型别名（如果未指定，ChatService 会自动选择唯一的模型）
    """

    models: Dict[str, OpenAIModelConfig] = Field(default_factory=dict)
    default_model: Optional[str] = None

    class Config:
        # 忽略未知字段
        extra = "ignore"

    @field_validator("models", mode="before")
    @classmethod
    def _parse_models_json(cls, v: Any) -> Any:
        """智能解析 models 字段（兼容 JSON 字符串格式）

        Args:
            v: 原始值（可能是 JSON 字符串或字典）

        Returns:
            Dict: 解析后的字典。

        Raises:
            ValueError: 当 JSON 格式错误时
        """
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return {}
            try:
                return json.loads(v)
            except json.JSONDecodeError as e:
                raise ValueError(f"llm.models 的 JSON 格式无效: {e}") from e
        return v
