from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ElasticsearchConfig(BaseModel):
    """Elasticsearch 连接配置模型

    该模型封装了 ES 客户端的所有连接参数，支持 HTTP/HTTPS、Basic Auth、API Key 以及
    复杂的 SSL/TLS 证书配置（含 mTLS 双向认证）。

    Attributes:
        hosts (List[str]): ES 节点地址列表，如 ["https://es-node-1:9200", "https://es-node-2:9200"]
        username (Optional[str]): Basic Auth 用户名
        password (Optional[str]): Basic Auth 密码
        api_key (Optional[str]): API Key (Base64编码)，优先级高于 Basic Auth
        request_timeout (float): 请求超时时间（秒），默认 10.0
        max_connections (int): 连接池最大连接数，默认 10
        max_retries (int): 最大重试次数，默认 3
        retry_on_timeout (bool): 超时是否重试，默认 True
        sniff_on_start (bool): 启动时是否嗅探集群节点（适合集群模式），默认 False
        sniff_on_connection_fail (bool): 连接失败时是否嗅探，默认 False
        sniffer_timeout (float): 嗅探超时时间，默认 60.0
        verify_certs (bool): 是否验证 SSL 证书，生产环境建议 True
        ca_certs (Optional[str]): 服务端 CA 证书路径（用于验证服务器身份）
        client_cert (Optional[str]): 客户端证书路径（用于 mTLS 双向认证）
        client_key (Optional[str]): 客户端私钥路径（用于 mTLS 双向认证）
    """

    # 连接配置
    hosts: List[str] = Field(..., description="ES 节点地址列表")

    # 认证配置 (Basic Auth)
    username: Optional[str] = Field(None, description="Basic Auth 用户名")
    password: Optional[str] = Field(None, description="Basic Auth 密码")

    # 认证配置 (API Key)
    api_key: Optional[str] = Field(None, description="API Key (Base64编码)")

    # 性能与超时
    request_timeout: float = Field(10.0, description="请求超时时间(秒)")
    max_connections: int = Field(10, description="连接池最大连接数")

    # 重试策略
    max_retries: int = Field(3, description="最大重试次数")
    retry_on_timeout: bool = Field(True, description="超时是否重试")

    # 节点嗅探 (Sniffing)
    sniff_on_start: bool = Field(False, description="启动时是否嗅探节点")
    sniff_on_connection_fail: bool = Field(False, description="连接失败时是否嗅探")
    sniffer_timeout: float = Field(60.0, description="嗅探超时时间")

    # SSL/TLS 配置
    verify_certs: bool = Field(True, description="是否验证服务端证书")
    # 证书路径配置 (支持绝对路径，或相对于 config 目录的相对路径)
    ca_certs: Optional[str] = Field(None, description="服务端 CA 证书路径")
    client_cert: Optional[str] = Field(None, description="客户端公钥证书路径 (.crt)")
    client_key: Optional[str] = Field(None, description="客户端私钥路径 (.key)")

    class Config:
        # 禁止额外字段，防止配置拼写错误
        extra = "forbid"
