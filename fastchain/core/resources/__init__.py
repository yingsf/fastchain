from __future__ import annotations

from .apollo import ApolloResource
from .base import (
    HealthStatus,
    Resource,
    ResourceHealth,
    ResourceManager,
    ResourceRegistry
)
from .events import Event, EventBus
from .limits import ResourceLimiter
from .watcher import BackgroundWatcher

__all__ = [
    # Base Abstractions (核心抽象)
    "ResourceManager",
    "HealthStatus",
    "ResourceHealth",
    "Resource",
    "ResourceRegistry",
    # Event System (事件系统)
    "Event",
    "EventBus",
    # Background Worker (后台任务基座)
    "BackgroundWatcher",
    # Concurrency Control (并发控制)
    "ResourceLimiter",
    # Built-in Resources (内置资源实现)
    "ApolloResource",
]
