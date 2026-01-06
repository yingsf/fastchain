from __future__ import annotations

import atexit
import json
import logging
import socket
import sys
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, override, Iterable, List

from loguru import logger as loguru_logger

from ..config.constants import AppConstants
from ..config.local_settings import LoggingSettings
from ..config.settings_manager import SettingsManager
from ..tracing.context import get_trace_id


class LogLevel(str, Enum):
    """统一日志级别枚举（兼容 Loguru 和 logging 模块）"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


# 默认日志格式字符串（Loguru 模板语法）
# 格式说明：
#   - <green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>: 绿色时间戳（毫秒精度）
#   - <level>{level}</level>: 日志级别（自动着色，如 ERROR 为红色）
#   - <cyan>{name}</cyan>: 日志记录器名称（通常是模块名）
#   - <cyan>{function}</cyan>: 函数名称
#   - <cyan>{line}</cyan>: 行号
#   - <magenta>{extra[trace_id]}</magenta>: 紫色 TraceID（分布式追踪）
#   - <level>{message}</level>: 日志消息（级别着色）
DEFAULT_LOG_FORMAT: str = (
    "[<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<magenta>{extra[trace_id]}</magenta>] "
    "- <level>{message}</level>"
)


@dataclass
class LoggingConfig:
    """核心日志运行时配置（不包含 Sink 配置）

    定义 Loguru 的全局行为参数，与具体的输出目标（Sink）无关

    Attributes:
        level (LogLevel): 日志级别，低于此级别的日志会被丢弃。默认值：INFO（生产环境推荐）
        format_str (str): 日志格式字符串（Loguru 模板语法）。默认值：DEFAULT_LOG_FORMAT（包含 TraceID）
        enqueue (bool): 是否启用异步队列（多线程环境推荐）。默认值：True（避免 GIL 阻塞主线程）
        backtrace (bool): 是否启用堆栈回溯（异常时显示完整调用链）。默认值：False（生产环境推荐关闭，减少日志量）
        diagnose (bool): 是否启用诊断模式（显示变量值）。默认值：False（生产环境禁止，避免泄漏敏感信息）
        enable_uvicorn (bool): 是否拦截 Uvicorn 日志。默认值：True（显式启用）

    Note:
        参数说明：
        - enqueue=True: 日志消息会先入队列，由后台线程写入（避免阻塞）。
        - backtrace=True: 异常时显示完整调用链（包括库代码）。
        - diagnose=True: 异常时显示局部变量的值（调试神器，但可能泄漏敏感信息）。

    Warning:
        - diagnose=True 会泄漏敏感信息（如密码、Token），生产环境禁止启用。
        - backtrace=True 会增加日志量（可能影响性能），仅在故障排查时启用。
        - format_str 必须包含 {extra[trace_id]}，否则 TraceID 注入失效。
    """

    level: LogLevel = LogLevel.INFO
    format_str: str = DEFAULT_LOG_FORMAT
    enqueue: bool = True
    backtrace: bool = False
    diagnose: bool = False
    enable_uvicorn: bool = True


def _parse_level(level_str: str) -> LogLevel:
    """解析日志级别字符串（大小写不敏感）

    Args:
        level_str (str): 日志级别字符串（如 "info", "DEBUG", " warning "）

    Returns:
        LogLevel: 对应的日志级别枚举

    Raises:
        ValueError: 当输入的级别字符串无效时抛出（包含所有有效选项的提示）
    """
    # 1. 标准化输入：去除空格并转为大写
    upper = (level_str or "").strip().upper()

    try:
        # 2. 尝试转换为 LogLevel 枚举（值匹配）
        return LogLevel(upper)
    except ValueError as exc:
        # 3. 转换失败，生成友好的错误消息
        # 列出所有有效的日志级别（如 "DEBUG, INFO, WARNING, ERROR, CRITICAL"）
        valid = ", ".join(log_level.name for log_level in LogLevel)
        raise ValueError(
            f"无效的日志级别: {level_str!r}. 有效选项: {valid}"
        ) from exc


def _trace_id_patcher(record: dict[str, Any]) -> None:
    """TraceID 注入补丁（Loguru patcher 回调）

    在每条日志记录生成时被调用，自动从上下文提取 TraceID 并注入到 extra 字段

    执行以下步骤：
        1. 检查 record 是否有 extra 字段（没有则创建）
        2. 检查 extra 是否已有 trace_id（没有则从上下文提取）
        3. 如果上下文也没有 TraceID，使用兜底值 "no-trace"

    Args:
        record (dict[str, Any]): Loguru 日志记录字典（包含 level, message, extra 等字段）
    """
    # 1. 确保 extra 字段存在（Loguru 保证存在，但此处防御式编程）
    if "extra" not in record:
        record["extra"] = {}

    # 2. 如果 extra 中没有 trace_id，则从上下文提取
    if "trace_id" not in record["extra"]:
        # 从 contextvars 中获取当前协程/线程的 TraceID
        tid = get_trace_id()
        # 如果上下文中也没有 TraceID，使用兜底值 "no-trace"
        record["extra"]["trace_id"] = tid if tid else "no-trace"


class SinkKind(str, Enum):
    """Sink 类型枚举（定义所有支持的日志输出目标）

    每个枚举值对应一种输出目标（Console、File、Graylog 等）

    扩展新 Sink 的步骤：
        1. 在此枚举中添加新类型（如 ELASTICSEARCH = "elasticsearch"）
        2. 创建对应的 SinkConfig 类（如 ElasticsearchSinkConfig）
        3. 实现安装函数（如 _install_elasticsearch_sink）
        4. 在 _SINK_REGISTRY 中注册（映射枚举值到安装函数）

    Note:
        - 枚举值使用小写字符串（与配置文件的 sinks 字段对应）
        - 继承自 str，可直接与字符串比较（如 "console" == SinkKind.CONSOLE）
    """
    CONSOLE = "console"
    FILE = "file"
    GRAYLOG = "graylog"


@dataclass
class ConsoleSinkConfig:
    """控制台 Sink 配置（输出到 sys.stdout）

    最简单的 Sink 类型，适合开发环境和容器化部署（Docker/Kubernetes）

    Attributes:
        enabled (bool): 是否启用此 Sink
            默认值：True（默认启用）
    """
    enabled: bool = True


@dataclass
class FileSinkConfig:
    """文件 Sink 配置（输出到本地文件，支持轮转和压缩）

    适合生产环境的日志持久化，支持自动轮转和压缩（节省磁盘空间）

    Attributes:
        enabled (bool): 是否启用此 Sink。默认值：True（默认启用）
        filename (Path | None): 日志文件路径。默认值：None（需在初始化时指定）
        rotation (str): 轮转策略（文件大小或时间间隔）。默认值："20 MB"（文件达到 20MB 时轮转）
        compression (str): 压缩算法（轮转后的文件压缩格式）。默认值："zip"（使用 ZIP 压缩）
    """
    enabled: bool = True
    filename: Path | None = None
    rotation: str = "20 MB"
    compression: str = "zip"


@dataclass
class GraylogSinkConfig:
    """Graylog Sink 配置（输出到 Graylog 服务器，GELF 格式）

    适合集中式日志管理，支持实时搜索、告警和可视化

    Attributes:
        enabled (bool): 是否启用此 Sink。默认值：False（需显式启用）
        host (str): Graylog 服务器地址。默认值："127.0.0.1"（本地测试）
        port (int): Graylog GELF UDP 端口。默认值：12201（Graylog 默认端口）
        protocol (str): 传输协议（仅支持 "udp"）。默认值："udp"（性能考虑）
        source (str | None): 日志来源标识（用于区分不同服务）。默认值：None（自动使用 logger 名称）

    Note:
        GELF 格式说明：
        - GELF = Graylog Extended Log Format（Graylog 官方格式）。
        - 基于 JSON，支持结构化日志（key-value 字段）。
        - 包含标准字段（version, host, timestamp, level, message）。
        - 支持自定义字段（以下划线 `_` 开头，如 `_user_id`）。

        UDP 协议优势：
        - 非阻塞：不会阻塞应用主线程（性能最优）。
        - 无连接：无需建立 TCP 连接（减少开销）。
        - 故障隔离：Graylog 宕机不会影响应用（日志丢失但不崩溃）。

        UDP 协议劣势：
        - 不可靠：网络拥塞时可能丢包（可接受，日志非关键数据）。
        - 无确认：无法确认日志是否成功投递。

        source 字段用途：
        - 用于区分不同服务的日志（如 "api-service", "worker-service"）。
        - 可在 Graylog 中按 source 过滤日志。
    """
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 12201
    protocol: str = "udp"
    source: str | None = None


# Sink 安装函数类型别名（便于类型检查）
# 接受参数：(logger_obj, logging_cfg, sink_cfg)
# 无返回值（仅执行副作用：调用 logger.add()）
SinkInstaller = Any


def _build_common_sink_args(cfg: LoggingConfig) -> Dict[str, Any]:
    """构建 Sink 通用参数（所有 Sink 共享的配置）

    提取 LoggingConfig 中与 Sink 无关的参数，避免重复代码

    Args:
        cfg (LoggingConfig): 核心日志配置对象

    Returns:
        Dict[str, Any]: Sink 通用参数字典，包含以下键：
            - format: 日志格式字符串。
            - level: 日志级别（字符串形式）。
            - enqueue: 是否启用异步队列。
            - backtrace: 是否启用堆栈回溯。
            - diagnose: 是否启用诊断模式。

    Note:
        这些参数会被传递给 loguru_logger.add() 方法。

        参数说明：
        - format: 控制日志输出格式（支持模板语法）。
        - level: 过滤低于此级别的日志。
        - enqueue: 启用后，日志消息会先入队列（避免阻塞）。
        - backtrace: 启用后，异常时显示完整调用链。
        - diagnose: 启用后，异常时显示变量值（调试用）。
    """
    # 返回通用参数字典（直接映射 LoggingConfig 的字段）
    return {
        "format": cfg.format_str,
        "level": cfg.level.value,
        "enqueue": cfg.enqueue,
        "backtrace": cfg.backtrace,
        "diagnose": cfg.diagnose,
    }


def _install_console_sink(
        logger_obj: Any,
        cfg: LoggingConfig,
        sink_cfg: ConsoleSinkConfig
) -> None:
    """安装控制台 Sink（输出到 sys.stdout）

    执行以下步骤：
        1. 检查是否启用（enabled=False 时直接返回）
        2. 构建通用参数（format, level 等）
        3. 调用 logger.add(sys.stdout, ...) 注册 Sink

    Args:
        logger_obj (Any): Loguru logger 实例。
        cfg (LoggingConfig): 核心日志配置。
        sink_cfg (ConsoleSinkConfig): 控制台 Sink 配置。

    Note:
        输出目标选择：
        - sys.stdout: 标准输出流（适合容器环境）
        - sys.stderr: 标准错误流（部分场景需要）

        彩色输出说明：
        - Loguru 会自动检测终端类型（TTY）
        - 支持 ANSI 颜色代码（如 <green>, <red>）
        - 容器环境可能不支持彩色（取决于日志收集器）
    """
    # 1. 检查是否启用（禁用时直接返回，不注册 Sink）
    if not sink_cfg.enabled:
        return

    # 2. 构建通用参数（format, level 等）
    common = _build_common_sink_args(cfg)

    # 3. 注册控制台 Sink（输出到 sys.stdout）
    logger_obj.add(sys.stdout, **common)


def _install_file_sink(
        logger_obj: Any,
        cfg: LoggingConfig,
        sink_cfg: FileSinkConfig
) -> None:
    """安装文件 Sink（输出到本地文件，支持轮转和压缩）

    执行以下步骤：
        1. 检查是否启用（enabled=False 时直接返回）
        2. 验证 filename 参数（为 None 时抛出异常）
        3. 构建通用参数（format, level 等）
        4. 调用 logger.add(filename, rotation=..., compression=...) 注册 Sink

    Args:
        logger_obj (Any): Loguru logger 实例
        cfg (LoggingConfig): 核心日志配置
        sink_cfg (FileSinkConfig): 文件 Sink 配置

    Raises:
        ValueError: 当 enabled=True 但 filename=None 时抛出

    Note:
        文件路径处理：
        - Path 对象会被转为字符串（Loguru 要求）
        - 支持相对路径（相对于当前工作目录）
        - 支持绝对路径（推荐，避免路径混乱）

        轮转和压缩说明：
        - rotation: 文件达到指定大小或时间时创建新文件
        - compression: 轮转后的文件自动压缩（节省空间）
        - Loguru 会自动处理文件命名（添加时间戳后缀）

    Warning:
        - 磁盘空间不足时，写入失败（需监控磁盘使用率）
        - 高频日志可能导致频繁轮转（影响性能）
        - 压缩会消耗 CPU 资源（可关闭压缩以提升性能）
    """
    # 1. 检查是否启用（禁用时直接返回，不注册 Sink）
    if not sink_cfg.enabled:
        return

    # 2. 验证 filename 参数（为 None 时抛出异常）
    if sink_cfg.filename is None:
        raise ValueError("当 enabled=True 时，FileSinkConfig.filename 不能为空")

    # 3. 构建通用参数（format, level 等）
    common = _build_common_sink_args(cfg)

    # 4. 注册文件 Sink（输出到指定文件）
    logger_obj.add(
        str(sink_cfg.filename),
        rotation=sink_cfg.rotation,
        compression=sink_cfg.compression,
        **common
    )


# Loguru 日志级别到 GELF 日志级别的映射表
# GELF 使用 Syslog 级别（0-7），数字越小越严重
_LOGLEVEL_TO_GELF: Dict[str, int] = {
    "CRITICAL": 2,
    "ERROR": 3,
    "WARNING": 4,
    "INFO": 6,
    "DEBUG": 7
}

# 全局 Socket 管理器（防止资源泄漏）
_GRAYLOG_SOCKETS: List[socket.socket] = []
# 线程锁（保护并发访问）
_GRAYLOG_LOCK = threading.Lock()


def _cleanup_graylog_sockets():
    """清理所有 Graylog Socket（atexit 回调）"""
    # 1. 加锁（保护并发访问全局列表）
    with _GRAYLOG_LOCK:
        # 2. 遍历所有 Socket 并关闭
        for sock in _GRAYLOG_SOCKETS:
            try:
                # 关闭 Socket（释放文件描述符）
                sock.close()
            except Exception:
                # 3. 静默失败（避免在退出时抛出异常）
                pass

        # 4. 清空全局列表（释放 Python 对象引用）
        _GRAYLOG_SOCKETS.clear()


# 注册 atexit 回调（进程退出时自动清理 Socket）
atexit.register(_cleanup_graylog_sockets)


def _install_graylog_sink(
        logger_obj: Any,
        cfg: LoggingConfig,
        sink_cfg: GraylogSinkConfig,
) -> None:
    """安装 Graylog Sink（输出到 Graylog 服务器，GELF 格式）

    执行以下步骤：
        1. 检查是否启用（enabled=False 时直接返回）
        2. 验证协议（仅支持 UDP）
        3. 清理旧 Socket（防止热重载时泄漏）
        4. 创建 UDP Socket（非阻塞模式）
        5. 定义 Sink 回调函数（格式化 GELF 并发送）
        6. 注册 Sink 到 Loguru

    Args:
        logger_obj (Any): Loguru logger 实例
        cfg (LoggingConfig): 核心日志配置
        sink_cfg (GraylogSinkConfig): Graylog Sink 配置

    Raises:
        ValueError: 当 protocol 不是 "udp" 时抛出
    """
    # 1. 检查是否启用（禁用时直接返回，不创建 Socket）
    if not sink_cfg.enabled:
        return

    # 2. 验证协议（仅支持 UDP）
    if sink_cfg.protocol.lower() != "udp":
        raise ValueError("Graylog sink 仅支持 protocol='udp'")

    # 3. 清理旧 Socket（防止热重载时泄漏）
    _cleanup_graylog_sockets()

    try:
        # 4. 创建 UDP Socket（非阻塞模式）
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setblocking(False)

        # 5. 将 Socket 添加到全局列表（用于 atexit 清理）
        with _GRAYLOG_LOCK:
            _GRAYLOG_SOCKETS.append(udp_sock)

    except Exception as e:
        # 6. Socket 创建失败，记录错误并返回（不注册 Sink）
        sys.stderr.write(f"[logging] 创建 Graylog Socket 失败: {e}\n")
        return

    # 7. 创建 Socket 级别的锁（保护并发访问单个 Socket）
    sock_lock = threading.Lock()

    # 8. 定义 Sink 回调函数（格式化 GELF 并发送）
    def graylog_sink(message: Any) -> None:
        """Graylog Sink 回调函数（将 Loguru 日志转换为 GELF 格式并发送）

        Args:
            message (Any): Loguru 消息对象（包含 record 字段）
        """
        try:
            # 1. 提取日志记录对象（Loguru 内部格式）
            record = message.record

            # 2. 提取 extra 字段（自定义字段，如 trace_id, user_id）
            extra: Mapping[str, Any] = record.get("extra", {}) or {}

            # 3. 提取日志级别并转换为 GELF 级别（Syslog 0-7）
            level_name: str = record["level"].name
            gelf_level = _LOGLEVEL_TO_GELF.get(level_name, 6)  # 默认 6 (INFO)

            # 4. 确定日志来源（按优先级尝试多个字段）
            host = (
                sink_cfg.source
                or extra.get("component")
                or record.get("name")
                or f"{AppConstants.DEFAULT_APP_NAME}"
            )

            # 5. 构造 GELF 消息体（JSON 格式）
            gelf_payload: Dict[str, Any] = {
                "version": "1.1",
                "host": host,
                "short_message": record["message"],
                "timestamp": record["time"].timestamp(),
                "level": gelf_level,
                "_logger": record.get("name"),
                "_module": record.get("module"),
                "_function": record.get("function"),
            }

            # 6. 添加 extra 中的自定义字段（以下划线 _ 开头）
            for key, value in extra.items():
                if value is None:
                    # 跳过 None 值（避免发送无意义字段）
                    continue
                # 添加自定义字段（如 _trace_id）
                gelf_payload[f"_{key}"] = value

            # 7. 序列化为 JSON（紧凑格式，不缩进）
            data = json.dumps(
                gelf_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")

            # 8. 发送到 Graylog 服务器（UDP）
            with sock_lock:
                udp_sock.sendto(data, (sink_cfg.host, sink_cfg.port))

        except Exception:
            # 9. 静默失败（避免日志系统崩溃），不记录错误（避免递归日志，导致死循环）
            pass

    # 10. 注册 Sink 到 Loguru
    common_args = _build_common_sink_args(cfg)

    # 调用 logger.add() 注册 Sink（传入回调函数）
    logger_obj.add(graylog_sink, **common_args)


# Sink 注册表（映射 SinkKind 枚举到安装函数），用于动态安装 Sink（根据配置文件的 sinks 字段）
_SINK_REGISTRY: Dict[SinkKind, SinkInstaller] = {
    SinkKind.CONSOLE: _install_console_sink,
    SinkKind.FILE: _install_file_sink,
    SinkKind.GRAYLOG: _install_graylog_sink
}

# 标准库 logging 级别到 Loguru 级别的映射表
_LOGGING_TO_LOGURU_LEVEL: Dict[int, str] = {
    logging.CRITICAL: LogLevel.CRITICAL.value,
    logging.ERROR: LogLevel.ERROR.value,
    logging.WARNING: LogLevel.WARNING.value,
    logging.INFO: LogLevel.INFO.value,
    logging.DEBUG: LogLevel.DEBUG.value,
    logging.NOTSET: LogLevel.DEBUG.value,
}

# Uvicorn 相关的 logger 名称列表（需拦截）
_UVICORN_LOGGER_NAMES = {
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "uvicorn.asgi",
    "uvicorn.lifespan",
    "uvicorn.config"
}

# 备份原始配置（用于恢复），解决拦截 logger 后，无法恢复原始配置（导致测试污染）
_INTERCEPT_ORIGINAL_CONFIG: Dict[str, tuple[list[logging.Handler], bool, int]] = {}
_UVICORN_ORIGINAL_CONFIG: Dict[str, tuple[list[logging.Handler], bool, int]] = {}


class _InterceptHandler(logging.Handler):
    """标准库 logging.Handler 适配器（将日志重定向到 Loguru）

    拦截标准库的日志记录，并通过 Loguru 输出（统一日志格式）

    工作原理：
        1. 继承自 logging.Handler（标准库接口）
        2. 重写 emit() 方法（处理日志记录）
        3. 将 logging 的日志级别转换为 Loguru 级别
        4. 调用 loguru_logger.log() 输出日志

    Note:
        使用场景：
        - 拦截第三方库的日志（如 SQLAlchemy, httpx）
        - 统一日志格式（避免多种格式混乱）
        - 集成到 Loguru 的 Sink 系统（自动路由到 File/Graylog）

        栈深度调整：
        - Loguru 通过栈帧（frame）定位日志的调用位置
        - 标准库拦截后，栈深度会增加（需手动调整 depth）
        - 使用 opt(depth=...) 跳过中间层（定位到真正调用位置）

    Warning:
        - 不要拦截 Loguru 自己的 logger（会导致递归）
        - 栈深度计算错误会导致日志位置不准确
        - 异常处理失败会调用 handleError（避免崩溃）
    """

    @override
    def emit(self, record: logging.LogRecord) -> None:
        """处理日志记录（核心方法）

        执行以下步骤：
            1. 转换日志级别（logging -> Loguru）
            2. 计算栈深度（定位真实调用位置）
            3. 调用 loguru_logger.log() 输出日志
            4. 异常处理（避免日志系统崩溃）

        Args:
            record (logging.LogRecord): 标准库日志记录对象

        Note:
            栈深度计算逻辑：
            - 从当前栈帧（currentframe）开始向上追溯
            - 跳过 logging 模块内部的栈帧（f_code.co_filename == logging.__file__）
            - 设置最大深度为 20（防止无限循环）

            异常信息传递：
            - record.exc_info: 包含异常的 traceback 信息
            - Loguru 会自动格式化异常（显示堆栈）

        Warning:
            - 栈深度计算失败会导致日志位置不准确（显示为 _InterceptHandler）
            - 异常处理失败会调用 handleError（记录到 sys.stderr）
        """
        try:
            # 1. 转换日志级别（logging.LEVEL -> "LEVEL"）
            level = _LOGGING_TO_LOGURU_LEVEL.get(record.levelno, LogLevel.INFO.value)

            # 2. 计算栈深度
            # 获取当前栈帧
            frame = logging.currentframe()
            # 初始深度（跳过 emit() 和 log()）
            depth = 2
            # 最大深度（安全保护）
            max_depth = 20

            # 3. 向上追溯栈帧，跳过 logging 模块内部的帧
            while frame and frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1
                if depth > max_depth:
                    break

            # 4. 调用 loguru_logger.log() 输出日志
            loguru_logger.opt(
                depth=depth,
                exception=record.exc_info,
            ).log(level, record.getMessage())

        except Exception:
            # 5. 异常处理（避免日志系统崩溃），handleError() 会将错误记录到 sys.stderr
            self.handleError(record)


def intercept_loggers(logger_names: Iterable[str]) -> None:
    """拦截标准库 logger 并重定向到 Loguru（公共 API）

    执行以下步骤：
        1. 创建 _InterceptHandler 实例
        2. 遍历所有 logger 名称
        3. 备份原始配置（如果尚未备份）
        4. 替换 handlers 为 _InterceptHandler
        5. 禁用传播（propagate=False，避免重复日志）

    Args:
        logger_names (Iterable[str]): 需要拦截的 logger 名称列表，例如：`["sqlalchemy.engine", "httpx"]`

    Note:
        幂等性保证：
        - 多次调用不会重复备份配置（通过字典去重）
        - 可以安全地在热重载时调用

        传播控制：
        - propagate=False: 禁止日志向父 logger 传播（避免重复输出）
        - 例如：`sqlalchemy.engine` 的日志不会传播到 `sqlalchemy`

    Warning:
        - 不要拦截 Loguru 自己的 logger（会导致递归）
        - 拦截后，原始的 handlers 会失效（无法恢复，除非调用 restore_loggers）
    """
    # 1. 创建 _InterceptHandler 实例（单例，所有 logger 共享）
    handler = _InterceptHandler()

    # 2. 遍历所有 logger 名称
    for name in logger_names:
        # 3. 获取 logger 实例（logging.getLogger 是单例）
        lg = logging.getLogger(name)

        # 4. 备份原始配置
        if name not in _INTERCEPT_ORIGINAL_CONFIG:
            # 备份三元组：(handlers 列表副本, propagate 标志, 日志级别)
            _INTERCEPT_ORIGINAL_CONFIG[name] = (lg.handlers[:], lg.propagate, lg.level)

        # 5. 替换 handlers 为 _InterceptHandler（仅保留单个 handler）
        lg.handlers = [handler]

        # 6. 禁用传播（避免日志向父 logger 传播）
        lg.propagate = False


def restore_loggers(logger_names: Iterable[str]) -> None:
    """恢复拦截的 logger 的原始配置（公共 API）

    执行以下步骤：
        1. 遍历所有 logger 名称
        2. 从备份字典中提取原始配置
        3. 恢复 handlers、propagate、level
        4. 从备份字典中删除条目

    Args:
        logger_names (Iterable[str]): 需要恢复的 logger 名称列表

    Note:
        使用场景：
        - 测试环境：测试结束后恢复原始配置（避免污染）
        - 热重载：重新初始化日志系统前恢复原始配置

        幂等性保证：
        - 如果 logger 未被拦截，直接跳过（不会抛出异常）
    """
    # 1. 遍历所有 logger 名称
    for name in logger_names:
        # 2. 检查是否在备份字典中（未拦截则跳过）
        if name in _INTERCEPT_ORIGINAL_CONFIG:
            # 3. 提取原始配置（三元组）
            handlers, propagate, level = _INTERCEPT_ORIGINAL_CONFIG[name]

            # 4. 获取 logger 实例
            lg = logging.getLogger(name)

            # 5. 恢复 handlers（使用备份的列表）
            lg.handlers = handlers

            # 6. 恢复 propagate 标志
            lg.propagate = propagate

            # 7. 恢复日志级别
            lg.setLevel(level)

            # 8. 从备份字典中删除条目（释放内存）
            del _INTERCEPT_ORIGINAL_CONFIG[name]


def enable_uvicorn_logging() -> None:
    """启用 Uvicorn 日志拦截（公共 API）

    执行以下步骤：
        1. 创建 _InterceptHandler 实例
        2. 遍历所有 Uvicorn logger 名称
        3. 备份原始配置（如果尚未备份）
        4. 替换 handlers 为 _InterceptHandler
        5. 禁用传播
        6. 设置默认日志级别为 INFO（如果未设置）

    Note:
        使用场景：
        - FastAPI/Uvicorn 应用：统一访问日志格式。
        - 集成到 Loguru 的 Sink 系统（自动路由到 File/Graylog）。

        Uvicorn logger 说明：
        - uvicorn: 主 logger（应用启动、停止等）。
        - uvicorn.error: 错误日志（ASGI 应用异常）。
        - uvicorn.access: 访问日志（HTTP 请求，如 "GET /api/users 200"）。
    """
    # 1. 创建 _InterceptHandler 实例（单例，所有 logger 共享）
    handler = _InterceptHandler()

    # 2. 遍历所有 Uvicorn logger 名称
    for logger_name in _UVICORN_LOGGER_NAMES:
        # 3. 获取 logger 实例
        lg = logging.getLogger(logger_name)

        # 4. 备份原始配置（如果尚未备份）
        if logger_name not in _UVICORN_ORIGINAL_CONFIG:
            _UVICORN_ORIGINAL_CONFIG[logger_name] = (
                lg.handlers[:],
                lg.propagate,
                lg.level
            )

        # 5. 替换 handlers 为 _InterceptHandler
        lg.handlers = [handler]

        # 6. 禁用传播
        lg.propagate = False

        # 7. 设置默认日志级别为 INFO（如果未设置）
        if lg.level == logging.NOTSET:
            lg.setLevel(logging.INFO)


def disable_uvicorn_logging() -> None:
    """恢复 Uvicorn 日志的原始配置（公共 API）

    执行以下步骤：
        1. 遍历备份字典中的所有 logger
        2. 恢复 handlers、propagate、level
        3. 清空备份字典

    Note:
        使用场景：
        - 测试环境：测试结束后恢复原始配置
        - 热重载：重新初始化日志系统前恢复原始配置
    """
    # 1. 遍历备份字典中的所有 logger
    for logger_name, (handlers, propagate, level) in _UVICORN_ORIGINAL_CONFIG.items():
        # 2. 获取 logger 实例
        lg = logging.getLogger(logger_name)

        # 3. 恢复 handlers
        lg.handlers = handlers

        # 4. 恢复 propagate 标志
        lg.propagate = propagate

        # 5. 恢复日志级别
        lg.setLevel(level)

    # 6. 清空备份字典（释放内存）
    _UVICORN_ORIGINAL_CONFIG.clear()


def build_logging_config_from_local(local_logging: LoggingSettings) -> LoggingConfig:
    """从本地配置构建 LoggingConfig 对象（配置工厂）

    执行以下步骤：
        1. 解析日志级别字符串（调用 _parse_level）
        2. 映射配置字段到 LoggingConfig
        3. 覆盖默认值（如果配置中指定了 format_str）

    Args:
        local_logging (LoggingSettings): 本地日志配置对象（从配置文件加载）

    Returns:
        LoggingConfig: 运行时日志配置对象
    """
    # 1. 构造 LoggingConfig 对象（映射配置字段）
    cfg = LoggingConfig(
        level=_parse_level(local_logging.level),
        enqueue=local_logging.enqueue,
        backtrace=local_logging.backtrace,
        diagnose=local_logging.diagnose,
        enable_uvicorn=local_logging.enable_uvicorn
    )

    # 2. 覆盖默认格式字符串（如果配置中指定）
    if local_logging.format_str:
        cfg.format_str = local_logging.format_str

    # 3. 返回运行时配置对象
    return cfg


def _build_sink_configs_from_local(
        local_logging: LoggingSettings,
        project_log_dir: Path,
        project_name: str,
        app_name: str | None = None,
        *,
        allow_file_sink: bool = True,
) -> Dict[SinkKind, ConsoleSinkConfig | FileSinkConfig | GraylogSinkConfig]:
    """从本地配置构建 Sink 配置字典（配置工厂）

    执行以下步骤：
        1. 解析 sinks 字段（逗号分隔的字符串 -> 集合）
        2. 根据 sinks 集合构建对应的 SinkConfig 对象
        3. 处理文件路径（拼接 project_log_dir 和 app_name）
        4. 返回 Sink 配置字典

    Args:
        local_logging (LoggingSettings): 本地日志配置对象
        project_log_dir (Path): 项目日志目录（如 /var/log/myapp）
        project_name (str): 项目名称（如 "myapp"）
        app_name (str | None): 应用名称（如 "api-server"，可选）
        allow_file_sink (bool): 是否允许文件 Sink（默认 True）。如果运行时目录不可访问，设置为 False（禁用文件 Sink）

    Returns:
        Dict[SinkKind, ...]: Sink 配置字典（key=SinkKind, value=对应的 SinkConfig）

    Note:
        Sink 启用逻辑：
        - sinks 字段包含对应的 Sink 名称（如 "console", "file"）
        - 对应的 SinkConfig 的 enabled=True
        - 特殊处理：文件 Sink 还需要 allow_file_sink=True

        文件命名规则：
        - 优先使用 app_name（如 "api-server.log"）
        - 如果 app_name 为空，使用 project_name（如 "myapp.log"）
    """
    # 1. 初始化 Sink 配置字典（空字典）
    sink_cfgs: Dict[
        SinkKind, ConsoleSinkConfig | FileSinkConfig | GraylogSinkConfig
    ] = {}

    # 2. 解析 sinks 字段（逗号分隔的字符串 -> 集合）
    sink_names = {
        name.strip().lower()
        for name in (local_logging.sinks or [])
        if name and name.strip()
    }

    # 3. 构建 Console Sink 配置
    if "console" in sink_names and getattr(local_logging, "console", None):
        if local_logging.console.enabled:
            sink_cfgs[SinkKind.CONSOLE] = ConsoleSinkConfig(enabled=True)

    # 4. 构建 File Sink 配置
    if (
        "file" in sink_names
        and getattr(local_logging, "file", None)
        and local_logging.file.enabled
        and allow_file_sink
    ):
        # 确定日志文件名（优先 app_name，否则 project_name）
        base_name = app_name or project_name
        # 拼接完整路径（如 /var/log/api-server.log）
        log_file = project_log_dir / f"{base_name}.log"

        # 构造 FileSinkConfig 对象
        sink_cfgs[SinkKind.FILE] = FileSinkConfig(
            enabled=True,
            filename=log_file,
            rotation=local_logging.file.rotation,
            compression=local_logging.file.compression
        )

    # 5. 构建 Graylog Sink 配置
    if (
        "graylog" in sink_names
        and getattr(local_logging, "graylog", None)
        and local_logging.graylog.enabled
    ):
        # 构造 GraylogSinkConfig 对象
        sink_cfgs[SinkKind.GRAYLOG] = GraylogSinkConfig(
            enabled=True,
            host=local_logging.graylog.host,
            port=local_logging.graylog.port,
            protocol=local_logging.graylog.protocol,
            source=local_logging.graylog.source
        )

    # 6. 返回 Sink 配置字典
    return sink_cfgs


def configure_loguru_logger(
        logging_cfg: LoggingConfig,
        sink_cfgs: Mapping[
            SinkKind, ConsoleSinkConfig | FileSinkConfig | GraylogSinkConfig
        ],
) -> Any:
    """配置 Loguru logger（核心配置逻辑）

    执行以下步骤：
        1. 移除所有默认 Sink（loguru_logger.remove）
        2. 配置 patcher（注入 TraceID）
        3. 遍历 Sink 配置字典，调用对应的安装函数
        4. 返回 Loguru logger 实例

    Args:
        logging_cfg (LoggingConfig): 核心日志配置对象
        sink_cfgs (Mapping[SinkKind, ...]): Sink 配置字典

    Returns:
        Any: Loguru logger 实例（全局单例）

    Note:
        默认 Sink 移除：
        - Loguru 默认会添加一个 Console Sink（输出到 sys.stderr）
        - 调用 remove() 移除所有默认 Sink（避免重复输出）

        故障隔离：
        - 单个 Sink 安装失败不会影响其他 Sink（try-except 捕获异常）
        - 错误会记录到 sys.stderr（避免依赖 Loguru）
    """
    # 1. 移除所有默认 Sink（清空 Loguru 的内部 Sink 列表）
    loguru_logger.remove()

    # 2. 配置 patcher（注入 TraceID）
    loguru_logger.configure(patcher=_trace_id_patcher)

    # 3. 遍历 Sink 配置字典，调用对应的安装函数
    for kind, cfg in sink_cfgs.items():
        # 从注册表中查找对应的安装函数
        installer = _SINK_REGISTRY.get(kind)

        if installer:
            try:
                # 调用安装函数（注册 Sink 到 Loguru）
                installer(loguru_logger, logging_cfg, cfg)
            except Exception as e:
                # 单个 Sink 安装失败，记录错误并继续（故障隔离）
                sys.stderr.write(f"[logging] Failed to install sink {kind}: {e}\n")

    # 4. 返回 Loguru logger 实例（全局单例）
    return loguru_logger


# 幂等性标记（防止重复初始化），解决多次调用 create_app_logger 会重复添加 Sink（导致日志重复输出）
_LOGGER_INITIALIZED = False


def create_app_logger(
        settings: SettingsManager,
        app_name: str | None = None,
        force: bool = False
) -> Any:
    """初始化全局日志系统（应用入口函数）

    执行以下步骤：
        1. 检查幂等性标记（已初始化且非强制模式时直接返回）
        2. 加载路径配置（项目日志目录、项目名称）
        3. 加载日志配置（LoggingSettings）
        4. 确保运行时目录可访问（创建日志目录）
        5. 构建 LoggingConfig 和 Sink 配置
        6. 配置 Loguru logger（调用 configure_loguru_logger）
        7. 启用 Uvicorn 拦截（如果配置中启用）
        8. 设置幂等性标记
        9. 返回 Loguru logger 实例

    Args:
        settings (SettingsManager): 配置管理器实例（访问本地配置和路径）
        app_name (str | None): 应用名称（用于日志文件命名，可选）
        force (bool): 是否强制重新初始化（默认 False）
            - False: 如果已初始化，直接返回（幂等性）
            - True: 即使已初始化，也重新初始化（热重载场景）

    Returns:
        Any: Loguru logger 实例（全局单例）

    Note:
        调用时机：
        - 应用启动时调用（在导入业务模块前）
        - 热重载时调用（传入 force=True）

        幂等性保证：
        - 第一次调用：完整初始化（创建目录、配置 Sink）
        - 后续调用：直接返回（避免重复初始化）
        - 强制模式：忽略幂等性标记（重新初始化）

        运行时目录处理：
        - 尝试创建日志目录（ensure_runtime_dirs）
        - 如果失败（如权限不足），禁用文件 Sink（降级到 Console Sink）
        - 错误会记录到 sys.stderr（便于调试）

    Warning:
        - 必须在应用启动早期调用（在导入业务模块前）
        - 运行时目录不可访问时，文件 Sink 会被禁用（日志不持久化）
        - 热重载时必须传入 force=True（否则不会重新初始化）
    """
    # 1. 声明全局变量（允许修改幂等性标记）
    global _LOGGER_INITIALIZED

    # 2. 幂等性检查
    if _LOGGER_INITIALIZED and not force:
        # 已初始化且非强制模式，直接返回（避免重复初始化）
        return loguru_logger

    # 3. 加载配置
    # 3.1 加载路径配置（项目日志目录、项目名称）
    paths = settings.paths
    # 3.2 加载日志配置（LoggingSettings）
    local_logging = settings.local.logging

    # 4. 确保运行时目录可访问，默认允许文件 Sink
    allow_file_sink = True
    try:
        # 尝试创建运行时目录（包括日志目录）
        paths.ensure_runtime_dirs()
    except OSError as exc:
        # 4.1 创建失败（如权限不足），禁用文件 Sink
        allow_file_sink = False

        # 4.2 记录警告到 sys.stderr（便于调试）
        try:
            sys.stderr.write(
                f"[logging] 无法访问运行时目录: {exc}. 文件 Sink 已禁用。\n"
            )
        except Exception:
            # 4.3 stderr 写入失败，静默失败（避免崩溃）
            pass

    # 5. 构建配置对象
    # 5.1 构建 LoggingConfig（核心日志配置）
    logging_cfg = build_logging_config_from_local(local_logging)

    # 5.2 构建 Sink 配置字典
    sink_cfgs = _build_sink_configs_from_local(
        local_logging=local_logging,
        project_log_dir=paths.log_dir,
        project_name=paths.project_name,
        app_name=app_name,
        allow_file_sink=allow_file_sink
    )

    # 6. 配置 Loguru logger
    logger_obj = configure_loguru_logger(logging_cfg, sink_cfgs)

    # 7. 启用 Uvicorn 拦截（可选）
    if logging_cfg.enable_uvicorn:
        enable_uvicorn_logging()

    # 8. 设置幂等性标记
    _LOGGER_INITIALIZED = True

    # 9. 返回 Loguru logger 实例
    return logger_obj
