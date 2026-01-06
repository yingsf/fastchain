"""API 路由根包（由 fastchain init 生成）

FastChain 会根据 Apollo 中 system.config.routers.modules 指定的模块路径，
递归扫描该包及其子模块，并自动注册所有导出 `router` 变量的模块。

约定：
- 在子模块中定义 router = fastapi.APIRouter(...)
"""
