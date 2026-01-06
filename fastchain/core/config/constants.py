from __future__ import annotations

from enum import Enum
from typing import Final

# ========== 配置文件相关常量 ==========

# 配置目录名称，约定在项目根目录下
CONFIG_DIR_NAME: Final[str] = "config"

# 配置文件名称，使用 TOML 格式以支持层次化配置和环境分离
ENV_FILE_NAME: Final[str] = "settings.toml"

# ========== 运行环境相关常量 ==========

# 默认运行环境类型（开发环境），适用于未显式指定环境变量时的回退值
DEFAULT_ENV_TYPE: Final[str] = "dev"

# 运行环境类型的环境变量名（例如：dev / test / prod），通过此环境变量可在运行时切换配置环境，无需修改代码
RUN_ENV_TYPE_VAR: Final[str] = "RUN_ENV_TYPE"

# ========== 项目根目录探测相关常量 ==========

# 项目根目录标记文件列表
# 用于向上递归搜索时判断是否到达项目根目录
# .git: 绝大多数项目都有的版本控制标记
# pyproject.toml: Python 项目的现代标准配置文件（PEP 518）
PROJECT_ROOT_MARKERS: Final[tuple[str, ...]] = (".git", "pyproject.toml")

# 项目根目录向上查找的最大层数（默认值）
# 设置为 10 可覆盖大部分嵌套深度场景，防止无限向上搜索导致性能问题
PROJECT_ROOT_SEARCH_MAX_DEPTH: Final[int] = 10

# 允许通过环境变量覆盖最大搜索深度，适用于特殊项目结构或容器化部署场景
PROJECT_ROOT_SEARCH_MAX_DEPTH_ENV_VAR: Final[str] = "ROOT_SEARCH_MAX_DEPTH"

# ========== 运行时目录名称 ==========

# 日志文件存储目录
LOG_DIR_NAME: Final[str] = "log"

# 数据文件存储目录（如缓存、临时文件、持久化数据等）
DATA_DIR_NAME: Final[str] = "data"

# ========== Dynaconf 环境变量前缀 ==========

# Dynaconf 会自动读取带有此前缀的环境变量并合并到配置中
# 例如：APP_CONFIG_APOLLO_SERVER_URL 会覆盖 settings.toml 中的 apollo_server_url
ENVVAR_PREFIX: Final[str] = "APP_CONFIG"

# ========== 敏感信息关键词（用于日志脱敏） ==========

# 以下关键词用于识别配置项或日志字段是否包含敏感信息。
# 设计考量：
# 1. 覆盖常见敏感数据类型：密码、密钥、令牌、支付信息等
# 2. 支持多种命名风格：password / passwd / pwd / secret_key 等
# 3. 避免过于泛化的词（如 "key"），以免误伤正常字段
# 4. 使用 frozenset 以提升查找性能（O(1) 成员检测）
SENSITIVE_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        # 密码相关
        "password",
        "passwd",
        "pwd",
        # 密钥与令牌
        "secret",
        "token",
        "credential",
        "auth",
        "apikey",
        "api_key",
        "access_key",
        "secret_key",
        # 身份认证
        "jwt",
        "oauth",
        "session",
        "cookie",
        # 敏感个人信息
        "ssn",
        "social_security",
        # 支付信息
        "credit_card",
        "card_number",
        "cvv",
        "cvc",
        "pin",
        # 加密密钥
        "encryption_key",
        "private_key",
        # 云服务密钥
        "secret_access_key",
        "aws_secret",
        "gcp_key",
        "azure_key",
    }
)


# ========== 安全配置相关常量 ==========
# 本地缓存加密密钥的环境变量名，若未设置，框架将使用 AppID 生成兜底密钥（仅提供基础隔离）
ENV_CACHE_ENCRYPTION_KEY: Final[str] = "ENCRYPTION_CACHE_KEY"


# ========== 资源名称 ==========
class ResourceName(str, Enum):
    APOLLO = "apollo-main"
    SQL_DB = "sql_db_manager"
    REDIS = "redis_manager"
    MONGO_DB = "mongo_manager"
    LLM = "llm_manager"
    PROMPT = "prompt_manager"
    SCHEDULER = "scheduler_manager"


class ResourceKind(str, Enum):
    """资源逻辑分类枚举

    用于标识不同类型的外部依赖资源，便于：
    - 资源的统一管理和查询（如按类型获取所有数据库资源）
    - 监控和告警分组（如单独监控所有 LLM 资源的健康状态）
    - 启动优先级的语义化分类（如配置中心优先于数据库启动）

    通过继承 str 和 Enum，该枚举值可直接序列化为 JSON，便于与监控系统集成
    """
    # 基础设施 (替换原来的 APOLLO，范围更广，包含配置、服务发现)
    INFRA = "infrastructure"
    # 关系型数据库 (SQL)
    DATABASE = "database"
    # 缓存 (Redis)
    CACHE = "cache"
    # 文档数据库 (Mongo 专用)
    DOCUMENT = "document"
    # 大模型客户端
    LLM = "llm"
    # 提示词管理 (PromptManager 专用)
    PROMPT = "prompt"
    # 业务组件 (Scheduler, Listeners)
    COMPONENT = "component"
    # 消息队列
    MESSAGE_BROKER = "message_broker"


# 应用级常量
class AppConstants:
    """
    应用级常量与默认值，不建议放到apollo中管理。因为这会引入两个致命的架构隐患“启动悖论”和“安全风险”，所以：
     - 本地配置：负责定义 “我是谁” 和 “怎么启动”。（App Name, Version, Apollo URL, AppID）
     - 远程配置：负责定义 “怎么工作”。（DB 连接, 超时时间, 业务开关）
    """
    # 默认应用名称 (当 Settings 和 参数都未指定时使用)
    DEFAULT_APP_NAME = "FastChain Enterprise"

    # 默认版本号
    DEFAULT_VERSION = "0.1.0"

    # 默认的 API 路由模块路径，这是框架的约定：路由代码默认放在 api 目录下
    DEFAULT_API_MODULE = "fastchain.api"
