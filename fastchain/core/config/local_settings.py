from __future__ import annotations

import locale
from pathlib import Path

import os
from functools import lru_cache
from typing import Any, Literal, Optional, List, Dict

from dynaconf import Dynaconf
from pydantic import BaseModel, Field, ConfigDict, field_validator, AnyHttpUrl

from .constants import (
    DEFAULT_ENV_TYPE,
    RUN_ENV_TYPE_VAR,
    ENVVAR_PREFIX,
    SENSITIVE_KEYWORDS,
)
from .path_settings import get_path_settings


# ------------------------------------------------------------------
# 日志配置相关 Pydantic 模型
# ------------------------------------------------------------------


class LoggingConsoleSettings(BaseModel):
    """控制台日志输出配置

    用于控制日志是否输出到标准输出（stdout/stderr）

    Attributes:
        enabled (bool): 是否启用控制台日志输出。默认为 True
    """
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True


class LoggingFileSettings(BaseModel):
    """文件日志输出配置

    用于控制日志文件的滚动策略和压缩方式

    Attributes:
        enabled (bool): 是否启用文件日志输出。默认为 True
        rotation (str): 日志文件滚动策略（如按大小或时间滚动）。默认为 "20 MB"
        compression (str): 日志文件压缩格式（如 zip、gz）。默认为 "zip"

    Note:
        - rotation 支持多种格式：按大小（"500 MB"）、按时间（"1 day"）等
        - compression 可选值包括 "zip"、"gz"、"bz2" 等
    """
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    rotation: str = "20 MB"
    compression: str = "zip"


class LoggingGraylogSettings(BaseModel):
    """Graylog 日志输出配置

    用于将日志发送到 Graylog 集中式日志管理系统

    Attributes:
        enabled (bool): 是否启用 Graylog 日志输出。默认为 False
        host (str): Graylog 服务器地址。默认为 "127.0.0.1"
        port (int): Graylog 服务器端口。默认为 12201（GELF UDP 协议）
        protocol (Literal["udp"]): 传输协议。当前仅支持 "udp"
        source (Optional[str]): 日志来源标识（如应用名、主机名）。默认为 None

    Note:
        - Graylog 适用于生产环境的集中式日志收集和分析
        - UDP 协议性能较高但不保证消息到达，适用于日志场景
        - source 字段可用于在 Graylog 中过滤和分组日志
    """
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 12201
    protocol: Literal["udp"] = "udp"
    source: Optional[str] = None


class LoggingSettings(BaseModel):
    """日志系统总配置

    聚合控制台、文件、Graylog 等多种日志输出方式的配置

    Attributes:
        level (str): 全局日志级别（如 DEBUG、INFO、WARNING、ERROR）。默认为 "INFO"
        enqueue (bool): 是否启用异步日志队列（避免阻塞主线程）。默认为 True
        backtrace (bool): 是否在异常日志中包含完整堆栈追踪。默认为 False
        diagnose (bool): 是否启用诊断模式（显示变量值）。默认为 False
        enable_uvicorn (bool): 是否拦截 Uvicorn 的日志。默认为 True
        format_str (Optional[str]): 自定义日志格式字符串。默认为 None（使用默认格式）
        sinks (List[str]): 启用的日志输出方式列表。默认为 ["console", "file"]
        console (LoggingConsoleSettings): 控制台日志配置
        file (LoggingFileSettings): 文件日志配置
        graylog (LoggingGraylogSettings): Graylog 日志配置

    Note:
        - enqueue=True 可提升高并发场景下的日志性能，避免 I/O 阻塞
        - backtrace 和 diagnose 应仅在开发环境启用，生产环境可能泄露敏感信息
        - sinks 列表中的值对应具体的日志处理器（console、file、graylog 等）
    """
    model_config = ConfigDict(extra="forbid")

    level: str = "INFO"
    enqueue: bool = True
    backtrace: bool = False
    diagnose: bool = False
    enable_uvicorn: bool = True
    format_str: Optional[str] = None

    # 使用 default_factory 避免可变默认参数共享
    sinks: List[str] = Field(default_factory=lambda: ["console", "file"])

    # 使用 default_factory 避免 Pydantic 模型实例共享
    console: LoggingConsoleSettings = Field(default_factory=LoggingConsoleSettings)
    file: LoggingFileSettings = Field(default_factory=LoggingFileSettings)
    graylog: LoggingGraylogSettings = Field(default_factory=LoggingGraylogSettings)


# ------------------------------------------------------------------
# .env 编码兼容兜底（Windows GBK / UTF-8 差异）
# ------------------------------------------------------------------


def _find_dotenv(start_dir: Path | None = None, filename: str = ".env") -> Path | None:
    """向上搜索 .env 文件路径。

    在 Windows 中文环境下，系统默认编码通常为 GBK/cp936。若 `.env` 文件实际为 UTF-8
    （例如包含中文注释、全角符号或某些 Unicode 字符），Dynaconf 内置的 dotenv 读取
    可能因未显式指定编码而触发 `UnicodeDecodeError`。

    本函数用于在 FastChain 内部“先行定位 .env”，并配合 `_safe_load_dotenv()` 以更稳妥的
    编码策略加载到 `os.environ`，从而实现跨平台“**不炸**”。

    Args:
        start_dir: 搜索起点目录。默认使用当前工作目录（Path.cwd()）。
        filename: dotenv 文件名。默认 ".env"。

    Returns:
        找到则返回路径，否则返回 None。
    """
    base = start_dir or Path.cwd()
    for parent in (base, *base.parents):
        candidate = parent / filename
        if candidate.is_file():
            return candidate
    return None


def _safe_load_dotenv(override: bool = False) -> Path | None:
    """以“跨平台不炸”为目标加载 .env 到进程环境变量。

    设计目标：
    1. **零中断兼容**：无论 `.env` 是否存在、是否包含异常字符，都不阻断启动。
    2. **跨平台一致**：优先按 UTF-8（含 BOM）读取；失败回退到系统首选编码/GBK；
       最后以替换非法字符兜底，保证不抛 `UnicodeDecodeError`。
    3. **不破坏既有约定**：默认不覆盖已存在的 `os.environ`（override=False）。

    实现方式：
    - 使用 Dynaconf 内置的 dotenv loader（其解析能力更完整），但我们自行以不同编码
      打开文件并将 stream 传入，以绕开 Windows 默认编码导致的解码失败。

    Args:
        override: 是否覆盖已存在的环境变量。

    Returns:
        找到并成功加载的 `.env` 路径；若不存在或加载失败则返回 None。
    """
    dotenv_path = _find_dotenv()
    if not dotenv_path:
        return None

    # Dynaconf vendor 的 python-dotenv（随 dynaconf 依赖一起安装）
    from dynaconf.vendor.dotenv.main import load_dotenv  # local import: avoid global side effects

    preferred = locale.getpreferredencoding(False) or ""
    encodings: tuple[str, ...] = tuple(dict.fromkeys((
        "utf-8-sig",
        "utf-8",
        preferred,
        "gbk",
    )))

    for enc in encodings:
        if not enc:
            continue
        try:
            with dotenv_path.open("r", encoding=enc) as f:
                # 通过 stream 传入，避免 loader 自行 open 时采用系统默认编码
                load_dotenv(stream=f, override=override)
            return dotenv_path
        except UnicodeDecodeError:
            continue
        except Exception:
            # 兜底：任何解析异常都不应阻断启动
            return None

    # 最后兜底：以替换非法字符方式读取，确保不炸
    try:
        with dotenv_path.open("r", encoding="utf-8", errors="replace") as f:
            load_dotenv(stream=f, override=override)
        return dotenv_path
    except Exception:
        return None


# ------------------------------------------------------------------
# 本地配置主模型
# ------------------------------------------------------------------


class LocalSettings(BaseModel):
    """本地基础配置（基于 Dynaconf 加载）

    本类封装应用启动所需的所有本地配置，包括：
    - 运行环境标识（dev/test/prod）
    - Apollo 配置中心连接参数
    - 日志系统配置

    配置加载优先级（从高到低）：
    1. 环境变量（带 ENVVAR_PREFIX 前缀，如 APP_CONFIG_APOLLO_SERVER_URL）
    2. settings.toml 中的环境特定配置（如 [dev]、[prod]）
    3. settings.toml 中的默认配置
    4. Pydantic 模型的字段默认值

    设计考量：
    - 使用 Pydantic 进行 Schema 验证，确保配置合法性
    - 必填字段（如 apollo_server_url）使用 ... 标记，启动时会自动校验
    - 提供敏感信息脱敏方法（model_dump_safe），避免日志泄露密码等
    - 所有配置均为只读（加载后不应修改），保证配置一致性

    Attributes:
        env (str): 当前运行环境标识。默认为 "dev"
        apollo_server_url (str): Apollo 配置中心地址（必填）
        apollo_app_id (str): Apollo appId（必填）
        apollo_cluster (str): Apollo 集群名。默认为 "default"
        apollo_namespace (str): 默认 Apollo namespace。默认为 "application"
        logging (LoggingSettings): 日志系统配置

    Methods:
        model_dump_safe: 返回脱敏后的配置字典（用于日志输出）

    Note:
        - apollo_server_url 和 apollo_app_id 为必填字段，未配置时会抛出 ValidationError
        - 在生产环境中，建议通过环境变量覆盖敏感配置（如密钥）
    """
    # Pydantic v2 ConfigDict，兼容任意类型（如 AnyHttpUrl）
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    env: str = Field(
        default=DEFAULT_ENV_TYPE,
        description="当前运行环境标识（dev/test/prod...）。",
    )

    # AnyHttpUrl 校验 URL 格式，防止 SSRF 或格式错误
    apollo_server_url: AnyHttpUrl = Field(
        ...,
        description="Apollo 配置中心地址（必填，禁止为空）。",
    )
    apollo_app_id: str = Field(
        ...,
        min_length=1,
        description="Apollo appId（必填，禁止为空）。",
    )
    apollo_cluster: str = Field(
        default="default",
        description="Apollo 集群名。",
    )
    apollo_namespace: str = Field(
        default="application",
        description="默认 Apollo namespace。",
    )

    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="日志系统配置。",
    )

    @field_validator("apollo_server_url", mode="before")
    @classmethod
    def validate_url(cls, v: Any) -> Any:
        # 简单兼容处理空字符串
        if isinstance(v, str) and not v.strip():
            raise ValueError("URL cannot be empty")
        return v

    def _mask_value(self, key: str, value: Any) -> Any:
        """对敏感字段值进行脱敏处理

        用于在日志输出时隐藏密码、密钥等敏感信息，防止泄露

        Args:
            key (str): 字段名（用于判断是否为敏感字段）
            value (Any): 字段值

        Returns:
            Any: 脱敏后的值（敏感字段返回掩码字符串，非敏感字段返回原值）

        Note:
            - 脱敏策略：
              - 短字符串（长度 <= 6）：全部替换为 "******"
              - 长字符串（长度 > 6）：显示前 2 位和后 2 位，中间替换为 "****"
              - 非字符串类型的敏感字段：统一返回 "*****"
            - 字段名匹配不区分大小写，支持驼峰和下划线命名风格
        """
        # 递归处理字典
        if isinstance(value, dict):
            return {k: self._mask_value(k, v) for k, v in value.items()}

        # 递归处理 Pydantic 模型
        if isinstance(value, BaseModel):
            return self._mask_value(key, value.model_dump())

        # 检查字段名是否包含敏感关键词（不区分大小写）
        if any(keyword in key.lower() for keyword in SENSITIVE_KEYWORDS):
            if isinstance(value, str) and value:
                if len(value) <= 6:
                    # 短密码全掩盖（如 "abc123" -> "******"）
                    return "******"
                else:
                    # 长密码显示首尾（如 "MySecretKey123" -> "My****23"）
                    return f"{value[:2]}****{value[-2:]}"
            # 非字符串类型的敏感字段（如整数密钥）统一掩盖
            return "*****"
        # 非敏感字段返回原值
        return value

    def model_dump_safe(self) -> Dict[str, Any]:
        """返回脱敏后的配置字典（用于日志输出）

        此方法会对所有字段调用 _mask_value() 进行脱敏处理，适用于在日志中输出配置信息，避免敏感信息泄露。

        Returns:
            Dict[str, Any]: 脱敏后的配置字典

        Note:
            - 推荐在日志输出时使用此方法，而不是直接使用 model_dump()
            - 此方法会遍历所有字段，对嵌套对象（如 logging）也会递归处理
        """
        # 使用 mode='json' 确保转为纯字典，配合递归脱敏
        raw = self.model_dump(mode="json")
        return {k: self._mask_value(k, v) for k, v in raw.items()}

    def __repr__(self) -> str:
        """返回对象的字符串表示（自动脱敏）

        重写 __repr__ 方法，确保在调试时打印配置对象不会泄露敏感信息

        Returns:
            str: 脱敏后的对象表示字符串

        Note:
            - 在 REPL 或日志中直接打印 LocalSettings 实例时会自动调用此方法
        """
        safe = self.model_dump_safe()
        return f"LocalSettings({safe})"


@lru_cache(maxsize=1)
def _create_dynaconf() -> Dynaconf:
    """创建 Dynaconf 实例（通过 lru_cache 缓存）

    Dynaconf 是一个分层配置管理库，支持：
    - 多环境配置（dev/test/prod）
    - 多种配置源（文件、环境变量、远程配置中心）
    - 配置合并和覆盖

    配置加载逻辑：
    1. 读取环境变量 RUN_ENV_TYPE（默认为 "dev"）
    2. 加载 settings.toml 文件（位置由 PathSettings 提供）
    3. 合并 .env 文件中的环境变量（如果存在）
    4. 应用带有 ENVVAR_PREFIX 前缀的环境变量覆盖

    Returns:
        Dynaconf: 配置实例（全局单例）

    Note:
        - 使用 lru_cache 确保 Dynaconf 只初始化一次，避免重复文件读取
        - 此函数会触发文件系统 I/O，应在应用启动阶段调用
        - 测试环境中应通过依赖注入传入 mock 配置，而不是直接调用此函数
    """
    # 获取项目路径配置（包含 config_file 的完整路径）
    # ------------------------------------------------------------------
    # 兼容加载 .env（跨平台：优先 UTF-8，失败回退到系统编码/GBK）
    #
    # 说明：
    # - Windows 中文环境默认编码通常为 GBK/cp936，若 `.env` 实际为 UTF-8，
    #   Dynaconf 内置 dotenv 读取可能触发 UnicodeDecodeError。
    # - FastChain 在此处先行以更稳健的编码策略注入 os.environ，然后让 Dynaconf
    #   禁用内置 dotenv（load_dotenv=False），实现“模板防呆 + 框架兜底”。
    # ------------------------------------------------------------------
    _safe_load_dotenv(override=False)

    # 获取项目路径配置（包含 config_file 的完整路径）
    paths = get_path_settings()

    # 读取运行环境标识（环境变量优先，默认为 "dev"）
    env = os.getenv(RUN_ENV_TYPE_VAR, DEFAULT_ENV_TYPE)

    # 创建 Dynaconf 实例
    # environments=True: 启用多环境支持（在 settings.toml 中使用 [dev]、[prod] 等节）
    # settings_files: 指定配置文件路径（转为字符串以兼容 Dynaconf 2.x）
    # load_dotenv=False: 由 FastChain 先行以 UTF-8 兼容方式加载 .env，再交由 Dynaconf 合并环境变量
    # merge_enabled=True: 允许多层配置合并（默认配置 + 环境特定配置 + 环境变量）
    # envvar_prefix: 只加载带有此前缀的环境变量（如 APP_CONFIG_APOLLO_SERVER_URL）
    return Dynaconf(
        env=env,
        environments=True,
        settings_files=[str(paths.config_file)],
        load_dotenv=False,
        merge_enabled=True,
        envvar_prefix=ENVVAR_PREFIX,
    )


def load_local_settings() -> LocalSettings:
    """加载本地配置并生成强类型 Pydantic 模型

    此函数是配置加载的入口点，执行以下步骤：
    1. 调用 _create_dynaconf() 获取 Dynaconf 配置实例
    2. 从 Dynaconf 中提取各字段值
    3. 使用 Pydantic 进行 Schema 验证并构造 LocalSettings 对象

    Returns:
        LocalSettings: 强类型配置对象

    Raises:
        ValidationError: 当必填字段缺失或字段值不符合约束时抛出

    Note:
        - 此函数会触发文件系统 I/O 和 Schema 验证，应在应用启动阶段调用
        - 返回的 LocalSettings 对象是只读的，不应在运行时修改
        - 在测试环境中，建议通过依赖注入传入 mock 的 LocalSettings 实例
    """
    # 获取 Dynaconf 配置实例（首次调用会初始化，后续调用返回缓存）
    dynaconf = _create_dynaconf()

    # 从 Dynaconf 中提取配置值，构建字典，使用 .get() 方法并提供默认值，避免 KeyError
    data: Dict[str, Any] = {
        "env": dynaconf.get("env", DEFAULT_ENV_TYPE),
        "apollo_server_url": dynaconf.get("apollo_server_url"),
        "apollo_app_id": dynaconf.get("apollo_app_id"),
        "apollo_cluster": dynaconf.get("apollo_cluster", "default"),
        "apollo_namespace": dynaconf.get("apollo_namespace", "application"),
        # 日志配置：获取 "logging" 节的字典，如果不存在则使用空字典，LoggingSettings 的默认值会自动填充缺失字段
        "logging": LoggingSettings(**(dynaconf.get("logging", {}) or {})),
    }

    # 使用 Pydantic 进行 Schema 验证并构造对象，如果必填字段缺失或字段值不合法，会抛出 ValidationError
    return LocalSettings(**data)
