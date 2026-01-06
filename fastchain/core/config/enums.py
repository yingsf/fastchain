from __future__ import annotations

from enum import Enum


class ConfigGroup(str, Enum):
    """逻辑配置分组枚举（与具体配置中心无关的抽象）

    本枚举用于在应用内部标识不同的配置域，与具体的配置中心实现（Apollo、Nacos 等）解耦
    每个枚举值代表一个逻辑配置分组，可映射到配置中心的 namespace 或 dataId

    设计考量：
    - 使用 str 枚举，便于序列化和调试（.value 直接是字符串）
    - 分组粒度应适中：既避免过度细化导致管理复杂，也避免单一分组过于庞大
    - 可根据项目需求扩展新分组，保持命名语义清晰

    当前定义的分组示例：
    - LLM: 大模型相关配置（模型 ID、API 端点、超时设置等）
    - DATABASE: 数据库连接和连接池配置
    - FEATURE_FLAGS: 特性开关配置（灰度发布、A/B 测试等）

    扩展指南：
    - 新增分组时，建议按业务域或技术栈划分（如 CACHE、MESSAGE_QUEUE、THIRD_PARTY_APIS）
    - 避免与已有分组语义重叠
    - 更新配置中心的 namespace 映射关系（在配置加载器中）

    Attributes:
        LLM (str): 大模型相关配置分组
        DATABASE (str): 数据库相关配置分组
        FEATURE_FLAGS (str): 特性开关配置分组
    """

    # 大模型配置（如 API 密钥、模型选择、超时参数等）
    LLM = "llm"

    # 数据库配置（如连接串、连接池大小、查询超时等）
    DATABASE = "database"

    # 特性开关（用于灰度发布、蓝绿部署等场景）
    FEATURE_FLAGS = "feature_flags"
