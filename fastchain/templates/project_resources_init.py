"""资源根包（由 fastchain init 生成）

FastChain 会根据 Apollo 中 system.config.resources.modules 指定的模块路径，
递归扫描该包及其子模块，自动发现并注册 Resource 子类。

你可以在这个包下新增文件（如 db.py、redis.py、llm.py 等），并定义资源类：
- 继承 fastchain.core.resources.base.Resource
- 构造函数签名建议为 __init__(self, settings, event_bus)
"""
