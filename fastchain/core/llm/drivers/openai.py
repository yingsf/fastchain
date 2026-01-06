from langchain_openai import ChatOpenAI
from ...llm.base.config import OpenAIModelConfig


def build_openai_model(config: OpenAIModelConfig) -> ChatOpenAI:
    """构建标准 OpenAI 客户端

    适用范围：
    - OpenAI 官方 API
    - Azure OpenAI Service
    - 兼容 OpenAI API 的第三方服务（如 OpenRouter、LocalAI 等）

    参数映射：
    - model: 模型 ID（例如 "deepseek"）
    - api_key: API 密钥（从 SecretStr 中提取，确保安全性）
    - base_url: API Base URL（用于兼容第三方服务）
    - organization: 组织 ID（OpenAI 企业账户使用）
    - temperature, max_tokens, top_p: 核心生成参数
    - timeout, max_retries: 网络相关配置
    - model_kwargs: 透传给底层 API 的额外参数（如 presence_penalty）
    - default_headers: 自定义 HTTP Headers

    Args:
        config (OpenAIModelConfig): OpenAI 模型配置对象。

    Returns:
        ChatOpenAI: LangChain 的 ChatOpenAI 实例。

    Note:
        该函数不处理 HTTP Client 的创建和复用，LangChain 内部会自动管理
    """
    # 从 SecretStr 中提取 API Key（如果配置了的话），使用 get_secret_value() 而不是直接访问，确保安全性
    api_key = config.api_key.get_secret_value() if config.api_key else None

    return ChatOpenAI(
        model=config.model_name,
        api_key=api_key,
        base_url=config.base_url,
        organization=config.organization,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
        timeout=config.timeout,
        max_retries=config.max_retries,
        top_p=config.top_p,
        model_kwargs=config.model_kwargs,
        default_headers=config.default_headers,
    )
