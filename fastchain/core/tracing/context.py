from __future__ import annotations

import uuid
from contextvars import ContextVar

# 当前请求的 trace_id，上下文默认值为 "-"
TRACE_ID: ContextVar[str] = ContextVar("trace_id", default="-")


def new_trace_id() -> str:
    """生成一个新的 trace_id 并写入上下文，返回该值"""
    tid = uuid.uuid4().hex
    TRACE_ID.set(tid)
    return tid


def get_trace_id() -> str:
    """获取当前上下文的 trace_id，没有则返回默认值 '-'"""
    return TRACE_ID.get()
