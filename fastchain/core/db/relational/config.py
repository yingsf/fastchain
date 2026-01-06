from __future__ import annotations

from pydantic import BaseModel, Field


class RelationalDBConfig(BaseModel):
    """关系型数据库配置模型（严格模式）

    严格模式设计哲学：
        - 所有字段均无默认值，必须由外部配置（如 Apollo 配置中心）显式提供
        - 通过 Pydantic 验证确保配置的完整性和正确性，启动时即可发现配置问题

    设计考量：
        - 生产环境不应依赖代码中的硬编码默认值，所有配置应集中管理
        - 严格模式可避免配置缺失导致的静默失败（如使用错误的默认值）

    Attributes:
        url (str): 数据库连接字符串（DSN），如 "postgresql+asyncpg://user:pass@host/db"
        echo (bool): 是否启用 SQL 调试日志输出（开发环境建议开启，生产环境关闭）
        pool_size (int): 连接池核心大小（仅对非 SQLite 数据库生效），最小值为 1
        max_overflow (int): 最大溢出连接数（超出 pool_size 后允许的额外连接数），最小值为 0
        pool_recycle (int): 连接回收时间（秒），-1 表示永不回收（不推荐），建议设置为 3600（1 小时）

    Note:
        - pool_size 和 max_overflow 仅对 PostgreSQL、MySQL 等支持连接池的数据库生效
        - SQLite 会自动使用 NullPool（无连接池），忽略这些配置
        - pool_recycle 用于防止数据库服务端主动关闭长时间空闲的连接
    """
    # 数据库连接字符串（DSN），示例：postgresql+asyncpg://user:pass@host:5432/dbname
    url: str = Field(..., description="数据库连接字符串(DSN)")

    # SQL 调试日志开关
    # True：输出所有 SQL 语句和连接池事件（仅开发环境推荐）
    # False：仅输出警告和错误日志（生产环境推荐）
    echo: bool = Field(..., description="启用SQL调试日志输出")

    # 连接池核心大小，ge=1 确保至少有一个连接
    pool_size: int = Field(..., ge=1, description="连接池大小")

    # 最大溢出连接数，ge=0 表示不允许负数
    max_overflow: int = Field(..., ge=0, description="最大溢出连接数")

    # 连接回收时间（秒），定义连接在池中的最大存活时间，超时后会被自动关闭并重新创建
    # -1 表示永不回收（不推荐，可能导致连接泄漏），推荐值 3600（1 小时），根据数据库服务端的 wait_timeout 调整
    pool_recycle: int = Field(..., ge=-1, description="连接回收时间")

    class Config:
        """Pydantic 配置类。"""
        # 禁止额外字段：如果配置中出现未定义的字段，Pydantic 会抛出 ValidationError
        extra = "forbid"
