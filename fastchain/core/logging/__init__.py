from __future__ import annotations

from .logger import (
    LogLevel,
    LoggingConfig,
    SinkKind,
    ConsoleSinkConfig,
    FileSinkConfig,
    GraylogSinkConfig,
    configure_loguru_logger,
    create_app_logger,
    build_logging_config_from_local,
    enable_uvicorn_logging,
    disable_uvicorn_logging,
    intercept_loggers,
    restore_loggers
)

__all__ = [
    # 基础类型
    "LogLevel",
    "LoggingConfig",
    # Sink 相关
    "SinkKind",
    "ConsoleSinkConfig",
    "FileSinkConfig",
    "GraylogSinkConfig",
    # 核心 API
    "configure_loguru_logger",
    "create_app_logger",
    "build_logging_config_from_local",
    "intercept_loggers",
    "restore_loggers",
    # Uvicorn 集成
    "enable_uvicorn_logging",
    "disable_uvicorn_logging",
]
