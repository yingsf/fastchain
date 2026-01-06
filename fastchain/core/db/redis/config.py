from __future__ import annotations

from pydantic import BaseModel, Field


class RedisConfig(BaseModel):
    """Redis 连接与连接池配置模型

    该配置模型用于封装 Redis 客户端的所有连接参数，支持通过 DSN 格式的 URL 进行统一配置，同时提供连接池、超时、编码等细粒度控制选项

    设计考虑：
    - 使用 Pydantic 进行配置验证，确保参数类型正确且符合约束
    - 所有字段均提供合理的默认值，降低配置复杂度
    - 支持热重载场景，配置变更时可通过 model_dump() 比对差异

    Attributes:
        url (str): Redis 连接 URL（DSN 格式），支持 redis:// 和 rediss://（SSL）协议
        max_connections (int): 连接池最大连接数，控制并发访问时的资源上限
        socket_timeout (float): Socket 读取超时时间（秒），防止长时间阻塞
        socket_connect_timeout (float): Socket 连接超时时间（秒），控制建连延迟上限
        retry_on_timeout (bool): 超时时是否自动重试，提升瞬时网络抖动的容错能力
        encoding (str): 字符编码格式，影响字符串数据的序列化/反序列化
        decode_responses (bool): 是否自动将 Redis 返回的字节流解码为字符串

    Note:
        在生产环境中，max_connections 应根据并发请求量和 Redis 服务端能力调整，避免连接池耗尽或过度占用服务端资源
    """

    # 连接串 (DSN)
    # 格式: redis://[[username]:[password]]@localhost:6379/0，或: rediss://... (SSL)
    url: str = Field(..., description="Redis 连接 URL (DSN 格式)")

    # 连接池配置
    max_connections: int = Field(..., description="连接池最大连接数")

    # Socket 读取超时时间，防止 Redis 服务端响应缓慢导致客户端线程长时间挂起
    socket_timeout: float = Field(..., description="Socket 读取超时时间（秒）")

    # Socket 建连超时时间，用于快速失败（fail-fast）策略，避免网络分区时长时间等待
    socket_connect_timeout: float = Field(..., description="Socket 建连超时时间（秒）")

    # 超时重试开关，启用后可提升瞬时网络抖动场景下的成功率
    retry_on_timeout: bool = Field(..., description="超时是否自动重试")

    # 编码配置，UTF-8 是 Redis 字符串数据的标准编码，确保跨语言兼容性
    encoding: str = Field(..., description="字符编码格式 (通常为 utf-8)")

    # 自动解码开关，启用后 get()/hget() 等操作直接返回 str 而非 bytes，简化业务代码
    decode_responses: bool = Field(..., description="是否自动解码为字符串")

    class Config:
        # 禁止额外字段，防止配置拼写错误导致的静默失败
        extra = "forbid"
