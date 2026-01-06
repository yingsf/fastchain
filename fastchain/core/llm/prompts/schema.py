from __future__ import annotations

from typing import Any, Dict, List, Literal

from pydantic import BaseModel, Field


class PromptMessageConfig(BaseModel):
    """单条消息模板配置

    用于定义 Prompt 中的单条消息，支持 Jinja2 模板语法

    Attributes:
        role (Literal["system", "user", "assistant"]): 消息角色
            - system: 系统指令，定义 AI 的行为和约束
            - user: 用户输入
            - assistant: AI 的历史回复（用于 Few-Shot 示例）
        content (str): 消息内容。支持 Jinja2 语法，例如 "Hello {{ name }}"
    """

    # 角色：system, user, assistant，这三个角色覆盖了 LLM 对话中的所有场景
    role: Literal["system", "user", "assistant"]

    # 内容：支持 Jinja2 语法 (e.g. "Hello {{ name }}")，在 PromptManager.render_messages 中会被替换为实际值
    content: str


class PromptTemplateConfig(BaseModel):
    """Prompt 模板完整配置（对应 Apollo 中的 Value）

    定义一个完整的 Prompt 模板，包含元数据、默认变量和消息列表
    支持多轮对话结构（System + User + Assistant），适配 Few-Shot Learning 等场景

    Attributes:
        metadata (Dict[str, Any]): 元数据（版本、作者、描述等），仅用于展示或审计
        default_variables (Dict[str, Any]): 默认变量值。如果调用时未传递某个变量，使用此处的默认值
        messages (List[PromptMessageConfig]): 消息列表。这是核心，支持 System/User/Assistant 多轮对话结构

    Note:
        该配置通常存储在 Apollo 配置中心，Key 格式为 "prompts.<template_id>"，PromptManager 会自动扫描并加载所有匹配前缀的模板
    """

    # 元数据 (版本、作者、描述等，仅用于展示或审计)，不参与 Prompt 渲染，主要用于配置管理和版本控制
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # 默认变量值 (如果调用时没传，就用这里的)
    # 例如：{"language": "Python", "style": "clean"}
    # 在 ChatService 调用时，用户传入的 variables 会覆盖这里的默认值
    default_variables: Dict[str, Any] = Field(default_factory=dict)

    # 消息列表：这是核心，支持 System/User/Assistant 多轮对话结构
    # 例如：
    # - System Message 定义 AI 的角色和约束
    # - User Message 包含实际的任务描述
    # - Assistant Message 可以用作 Few-Shot 示例
    messages: List[PromptMessageConfig]

    class Config:
        # 忽略未知字段，防止配置更新时因新增字段导致解析失败
        extra = "ignore"
