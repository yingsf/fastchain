from __future__ import annotations

from pydantic import BaseModel, Field


class MongoConfig(BaseModel):
    """MongoDB 连接与 Beanie ORM 配置模型

    该配置类封装了 MongoDB 连接所需的所有参数，包括连接 URI、数据库名称、连接池配置以及 Beanie 模型自动发现的包路径

    Attributes:
        uri (str): MongoDB 连接字符串，支持标准 mongodb:// 或 mongodb+srv:// 协议
        database (str): 目标数据库名称。默认为 "fastchain"
        models_package (str): Python 包路径，用于自动扫描并注册 Beanie Document 模型
        min_pool_size (int): Motor 连接池最小连接数，用于保持常驻连接以降低冷启动延迟
        max_pool_size (int): Motor 连接池最大连接数，限制并发连接数以避免耗尽数据库资源
        timeout_ms (int): 服务器选择超时时间（毫秒），用于控制连接建立和故障检测的最大等待时间

    Note:
        - 该配置类采用 Pydantic 的严格模式（extra="forbid"），拒绝未定义的字段，以防止配置错误静默传播
        - 连接池参数需根据实际负载和数据库资源进行调优，过小会导致连接争用，过大会占用过多数据库连接资源
    """

    # 以下信息都在apollo的system.config.mongodb下配置

    # 连接信息
    uri: str = Field(..., description="MongoDB 连接 URI")
    database: str = Field(..., description="Database 名")

    # 自动发现模型的包路径（Beanie 的关键配置）
    # 该路径会被 ModelRegistry 递归扫描，所有继承自 beanie.Document 的类都会被自动注册
    models_package: str = Field(..., description="用于扫描 Beanie Document 模型的 Python 包路径")

    # 模型发现并发度
    model_discovery_concurrency: int = Field(..., ge=1, description="模型模块导入的最大并发线程数")

    # 连接池配置（Motor/PyMongo）
    min_pool_size: int = Field(..., description="连接池最小连接数")

    # max_pool_size 设为 100 是基于典型 Web 应用的并发需求，需根据实际 QPS 和响应时间调整
    max_pool_size: int = Field(..., description="连接池最大连接数")

    # 其它选项
    timeout_ms: int = Field(..., description="服务器选择超时时间（毫秒）")

    class Config:
        # 拒绝未定义字段，防止拼写错误或废弃字段静默通过验证
        extra = "forbid"
