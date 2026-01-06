from __future__ import annotations

import json
import re
from typing import Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

# 设置最大处理长度 (例如 1MB)，异常的大文本导致正则回溯卡死
MAX_TEXT_LENGTH = 1_000_000


class OutputParser:
    """LLM 输出解析器工具箱

    负责将 LLM 的原始文本输出（可能包含 Markdown 代码块、解释性文字等）转换为强类型的 Pydantic 对象

    核心流程：
    1. 安全检查：检查输入文本长度
    2. 文本清洗：从 Markdown 或混合文本中提取纯 JSON 字符串
    3. JSON 解析：解析为 Python 字典并校验类型
    4. 模型验证：使用 Pydantic 构造强类型对象

    设计考量：
    - 鲁棒性：处理 LLM 常见的输出模式（代码块、纯文本、嵌套结构）
    - 安全性：限制输入长度，防止正则回溯攻击 (ReDoS) 和内存耗尽
    - 可观测性：提供清晰的错误信息，便于 Self-Healing 机制修正

    Methods:
        extract_json_from_markdown: 从混合文本中提取 JSON 字符串
        parse: 核心解析方法，Text -> Pydantic Model
        get_format_instructions: 生成给 LLM 看的 JSON Schema 指令
    """

    @staticmethod
    def extract_json_from_markdown(text: str) -> str:
        """从 Markdown 文本中提取 JSON 字符串

        算法优先级：
        1. Markdown JSON 代码块 (```json ... ```)
        2. 通用代码块 (``` ... ```)
        3. 纯文本中的首尾花括号 ({ ... })

        Args:
            text (str): LLM 的原始输出文本

        Returns:
            str: 提取出的 JSON 字符串（未解析）

        Raises:
            ValueError: 当无法从文本中提取出有效的 JSON 结构时

        Note:
            - 使用贪婪匹配 (.*) 支持嵌套的花括号结构
            - 自动处理多行文本 (re.DOTALL)
        """
        if len(text) > MAX_TEXT_LENGTH:
            raise ValueError(f"输入文本超过最大长度 {MAX_TEXT_LENGTH} 个字符")

        text = text.strip()

        # 正则逻辑
        # 1. (?:json)? : 可选的语言标记
        # 2. \s* : 允许空白
        # 3. (\{.*\}) : 贪婪匹配花括号内容，确保捕获嵌套结构 (如 {"a": {"b": 1}})
        # 4. re.DOTALL : 让 . 匹配换行符
        pattern = r"```(?:json)?\s*(\{.*\})\s*```"
        match = re.search(pattern, text, re.DOTALL)

        if match:
            return match.group(1)

        # 降级策略：寻找最外层的花括号
        # 注意：如果文本包含多个独立的 JSON 对象（如 {"a":1} ... {"b":2}）
        # 这种启发式方法会截取整个范围。对于 LLM 单次回答场景，通常假设只有一个主 JSON 输出。
        first_brace = text.find("{")
        last_brace = text.rfind("}")

        if first_brace != -1 and last_brace != -1:
            # 简单的合法性预判：结束必须在开始之后
            if last_brace > first_brace:
                return text[first_brace: last_brace + 1]

        # 提取失败直接抛出异常，而不是返回原文本
        # 返回原文本会导致后续 json.loads 报出令人困惑的 "Expecting value..." 错误
        raise ValueError("在输出中未找到 JSON 代码块。")

    @classmethod
    def parse(cls, text: str, model: Type[T]) -> T:
        """核心解析方法：Text -> Dict -> Pydantic Model

        流程：
        1. 提取 JSON 字符串
        2. 解析 JSON 为字典（带类型检查）
        3. Pydantic 验证

        Args:
            text (str): LLM 的原始输出文本
            model (Type[T]): 目标 Pydantic Model 类型

        Returns:
            T: 验证通过的 Pydantic 对象实例

        Raises:
            ValueError: 当 JSON 格式错误或提取失败时
            ValidationError: 当数据不符合 Pydantic Schema 时
        """
        try:
            clean_text = cls.extract_json_from_markdown(text)
            data = json.loads(clean_text)
        except ValueError as e:
            # 统一包装为 ValueError，方便上层捕获并触发重试
            raise ValueError(f"JSON格式无效: {str(e)}") from e

        # 类型防御：确保解析结果是字典
        # LLM 有时会输出列表 [1, 2] 或基础类型 "value"，这会导致 model_validate 报错信息不直观
        if not isinstance(data, dict):
            raise ValueError(f"期望得到 JSON 对象 (dict)，实际得到 {type(data).__name__}")

        # 不需要捕获 ValidationError 再重新 raise，直接让 Pydantic 的异常冒泡即可，它包含了最详细的字段错误信息
        return model.model_validate(data)

    @staticmethod
    def get_format_instructions(model: Type[T]) -> str:
        """生成给 LLM 看的指令，告诉它应该输出什么 schema

        输出示例：
            "The output must be a valid JSON object adhering to the following schema:
            ```json
            { ... schema ... }
            ```
            Please return the JSON object directly."

        Args:
            model (Type[T]): 目标 Pydantic Model 类型

        Returns:
            str: 格式化的指令文本

        Note:
            该方法不再强制禁止 Markdown 代码块，因为解析器已经支持提取代码块
            允许代码块通常能让 LLM 输出更稳定的格式
        """
        # model_json_schema() 默认包含 $defs，这对嵌套模型至关重要
        schema = model.model_json_schema()

        # 仅移除最外层的冗余描述，保留核心结构
        reduced_schema = {
            "type": "object",
            "properties": schema.get("properties", {}),
            "required": schema.get("required", []),
            # 保留定义，防止引用丢失
            "$defs": schema.get("$defs", {})
        }

        # 允许（甚至鼓励）使用 Markdown 格式
        return (
            "The output must be a valid JSON object strictly adhering to the following schema:\n"
            f"```json\n{json.dumps(reduced_schema, indent=2, ensure_ascii=False)}\n```\n"
            "You usually respond with a JSON object wrapped in a markdown code block."
        )
