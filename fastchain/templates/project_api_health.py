"""健康检查路由（示例）

该文件的目的：
- 给你一个最小可运行的路由示例
- 让 FastChain 的路由自动发现机制能找到至少一个 router（便于确认骨架可用）
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    """健康检查（最小示例）"""
    return {"status": "ok"}
