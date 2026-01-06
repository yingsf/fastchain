from __future__ import annotations

from typing import Dict, Any

from fastapi import FastAPI
from loguru import logger

from ..config.constants import ResourceName
from ..config.settings_manager import SettingsManager
from ..listeners.audit import AuditEventListener
from ..llm.service import ChatService


def _resolve_dependencies(rm: Any) -> Dict[str, Any]:
    """从资源管理器中解析业务服务所需的底层资源依赖

    Args:
        rm (Any): 资源管理器实例，需提供 get_resource(name: str) 接口用于按名称获取已注册资源

    Returns:
        Dict[str, Any]: 依赖字典，键为内部变量名，值为对应的资源实例对象。该字典会被传递给业务服务的构造函数，用于依赖注入

    Raises:
        KeyError: 当任何必需资源未在资源管理器中注册时抛出。异常消息包含缺失资源的具体名称

    Note:
        - 此函数的设计哲学是"宁可启动失败，也不要带着残缺依赖运行"。
        - 若未来需要支持可选依赖（如某些功能在资源缺失时自动降级），建议在此处引入配置标志来区分必需和可选资源，而不是简单地捕获 KeyError
    """
    dependencies = {}

    # 定义业务服务所需的必需资源映射表
    # ChatService 的代码逻辑（__init__）决定了它需要什么。目前的 ChatService 只需要这三个
    # 假设将来升级了 ChatService，想用 Redis 做会话缓存，在这里添加 "redis_mgr": ResourceName.REDIS 就可以了
    required_resources = {
        "llm_mgr": ResourceName.LLM,
        "prompt_mgr": ResourceName.PROMPT,
        "db_mgr": ResourceName.SQL_DB
    }

    # 逐个尝试从资源管理器获取必需资源
    for var_name, res_name in required_resources.items():
        try:
            dependencies[var_name] = rm.get_resource(res_name)
        except KeyError:
            # 若资源未注册，抛出带有具体资源名称的异常
            raise KeyError(f"缺少必需资源: '{res_name}'")

    return dependencies


def _try_wire_scheduler(rm: Any) -> None:
    """尝试装配调度器依赖

    检查 SchedulerManager 是否已加载，如果存在，则尝试将 RedisManager 注入其中。这是实现"自适应分布式锁"的关键

    Logic:
    1. 检查 SchedulerManager 是否存在 (由配置 scheduler.enabled 决定)
    2. 如果存在，检查 RedisManager 是否存在 (由配置 redis.enabled 决定)
    3. 如果两者都在，执行注入 (scheduler.set_redis_manager)，激活集群模式
    4. 如果 Redis 缺席，什么都不做，Scheduler 保持默认的本地模式 (Local Mode)
    """
    try:
        # 1. 获取调度器资源
        scheduler_mgr = rm.get_resource(ResourceName.SCHEDULER)
    except KeyError:
        # 调度器未启用，无需进行任何装配
        return

    try:
        # 2. 获取 Redis 资源
        redis_mgr = rm.get_resource(ResourceName.REDIS)

        # 3. 注入依赖
        scheduler_mgr.set_redis_manager(redis_mgr)
        logger.debug("🔗 SchedulerManager: Redis 依赖注入成功 (Cluster Mode Ready)")
    except KeyError:
        # Redis 未启用，这是允许的，调度器将运行在无锁模式
        logger.debug("SchedulerManager: Redis 未启用，运行于 Local Mode")


async def wire_services(server: FastAPI) -> None:
    """执行业务服务装配与依赖注入（在 FastAPI 应用启动时调用）

    这是应用启动流程的第五阶段（Phase 5: 业务钩子），在 FastAPI 的 on_startup 事件中被触发
    该函数负责将已初始化的底层资源（LLM、数据库、提示词管理器等）组装成可用的业务服务实例，并启动事件监听器来处理异步审计任务

    装配流程分为四个阶段：
    1. 依赖解析（Dependency Resolution）：从资源管理器提取业务服务所需的核心资源
    2. 服务实例化（Service Instantiation）：创建业务服务实例并注入依赖
    3. 启动监听器（Start Listeners）：激活事件监听器，使其开始接收和处理事件
    4. 资源互联（Resource Wiring）：处理资源之间的弱依赖关系 (如 Scheduler -> Redis)
    5. 状态挂载（State Mounting）：将服务和监听器挂载到 FastAPI 应用的全局状态中

    Args:
        server (FastAPI): FastAPI 应用实例。通过 server.state 可以访问全局状态对象，包括资源管理器、配置管理器、和事件总线

    Raises:
        RuntimeError: 当依赖解析或服务实例化失败时抛出，中断应用启动流程
        KeyError: 当必需资源未在资源管理器中注册时抛出（来自 _resolve_dependencies）
    """
    # 从应用状态中获取资源管理器和配置管理器
    # 这些对象在 create_app() 的前几个阶段（1-4）已经初始化完成
    rm = server.state.resource_manager
    settings: SettingsManager = server.state.settings

    try:
        # 1. 依赖解析
        # 从资源管理器中提取业务服务所需的核心资源
        # 此步骤会进行严格的依赖校验：若任何必需资源缺失，会抛出 KeyError 并立即跳转到异常处理逻辑
        deps = _resolve_dependencies(rm)

        # 2. 服务实例化
        # 创建 ChatService 实例，这是应用的核心业务服务，负责处理对话请求
        chat_service = ChatService(
            manager=deps["llm_mgr"],
            event_bus=server.state.event_bus,
            prompt_manager=deps["prompt_mgr"],
            # 动态配置管理器，支持运行时配置更新
            settings=settings
        )

        # 3. 启动监听器
        # 创建审计事件监听器，负责异步记录用户请求日志到数据库
        audit_listener = AuditEventListener(
            # 数据库管理器，用于持久化审计日志
            db_manager=deps["db_mgr"],
            # 事件总线，监听器通过它接收事件
            event_bus=server.state.event_bus
        )

        # 关键步骤：显式启动监听器，开始接收和处理事件
        await audit_listener.start()

        # 4. 资源互联
        # 尝试为调度器注入 Redis 依赖
        # 这一步是完全可选的，不会因为 Redis 缺失而阻断启动
        _try_wire_scheduler(rm)

        # 5. 状态挂载
        # 将服务和监听器挂载到 FastAPI 应用的全局状态（server.state）中
        server.state.chat_service = chat_service
        server.state.audit_listener = audit_listener

        logger.success("⚡ 服务装配完成 & 监听器已启动.")

    except KeyError as e:
        logger.critical(f"服务装配失败: {e}")
        raise RuntimeError(f"装配已中止: {e}") from e

    except Exception as e:
        logger.critical(f"服务装配过程中出现未预期的错误: {e}")
        raise e
