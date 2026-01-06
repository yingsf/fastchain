from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class StreamChunk(BaseModel):
    """流式响应片段数据模型

    统一封装流式传输过程中的各种事件片段，供 Service 层返回给 Router 层。
    必须使用 BaseModel 以支持 model_dump() 序列化方法。

    Attributes:
        type (Literal["token", "error", "end"]): 片段类型
        content (Optional[str]): 文本内容
        error (Optional[str]): 错误信息
        metadata (Dict[str, Any]): 附加元数据
    """
    type: Literal["token", "error", "end"] = Field(..., description="片段类型")
    content: Optional[str] = Field(None, description="文本内容")
    error: Optional[str] = Field(None, description="错误信息")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")
