from __future__ import annotations

from fastapi import FastAPI
from loguru import logger

from .core.config.constants import ResourceName
from .core.resources import ResourceLimiter, ApolloResource
from .core.server import bootstrap, wiring
from .core.server.app import FastChainApp


def _init_infrastructure() -> FastChainApp:
    """初始化基础架构设施（Phase 1: Infrastructure）

    创建 FastChainApp 工厂实例，加载应用版本号，为后续配置引导和资源装配阶段做准备
    此阶段仅初始化最基本的对象（工厂实例、版本号），不涉及任何外部依赖（如网络 I/O、数据库连接）或复杂的业务逻辑

    Returns:
        FastChainApp: 应用工厂实例

    Note:
        - 此函数必须在整个启动流程的最前端调用，确保后续阶段有可用的工厂实例
        - 函数名以下划线开头（_init_infrastructure）表示这是内部函数，不应被外部模块直接调用
    """
    # 从包元数据中读取版本号，若包未安装（如开发环境）则使用默认值，版本号会被用于健康检查接口、日志输出等场景
    app_version = bootstrap.get_app_version()

    # 创建并返回应用工厂实例
    return FastChainApp(version=app_version)


def _bootstrap_configuration(app_factory: FastChainApp) -> None:
    """执行配置引导（Phase 2: Bootstrap）

    从远程配置中心（Apollo）同步拉取应用配置，或在网络不可用时回退到本地缓存
    此阶段采用"失败即退出"（Fail-fast）策略：若配置拉取完全失败（网络和缓存均不可用），应用将立即抛出异常并退出

    Args:
        app_factory (FastChainApp): 应用工厂实例，包含 settings（配置管理器）和 event_bus（事件总线）

    Raises:
        RuntimeError: 当配置引导失败时抛出，包含详细的错误原因。此异常会中断应用启动流程，确保应用不会以不完整配置继续运行

    Note:
        - bootstrap.run_bootstrap_in_thread 内部会创建新线程和新事件循环，执行异步的配置拉取逻辑
        - 异常会在线程中捕获并传播到主线程，确保配置拉取失败时应用能够正确退出（而非静默失败）
    """
    settings = app_factory.settings

    try:
        # 在独立线程中执行配置拉取（线程隔离模式）
        # 此函数会阻塞当前线程，直到配置拉取完成或失败
        # 若拉取失败（网络 + 缓存都不可用），会抛出 RuntimeError
        bootstrap.run_bootstrap_in_thread(settings)

    except Exception as e:
        logger.critical(f"引导执行失败: {e}")
        raise RuntimeError("由于引导失败,应用程序启动已中止。") from e


def _register_core_resources(app_factory: FastChainApp) -> None:
    """注册核心资源（Phase 3: Core Resources）

    注册应用运行时必需的核心资源（如 Apollo 配置客户端），并将其添加到资源管理器中
    核心资源通常具有最高启动优先级（startup_priority=0），确保在业务资源（如数据库）之前启动，以便业务资源可以从配置中心读取初始化参数

    Args:
        app_factory (FastChainApp): 应用工厂实例，用于访问 settings（配置管理器）和 event_bus（事件总线）
    """
    settings = app_factory.settings
    event_bus = app_factory.event_bus

    # 从已加载的配置中提取 Apollo 资源的限流和轮询参数
    # bootstrap.load_apollo_params 会进行类型校验和默认值回退，确保参数合法
    apollo_params = bootstrap.load_apollo_params(settings)

    # 实例化 Apollo 资源，配置命名空间、轮询间隔和限流策略
    apollo = ApolloResource(
        settings=settings,
        name=ResourceName.APOLLO,
        namespaces=[settings.local.apollo_namespace],
        poll_interval=apollo_params["poll_interval"],
        limiter=ResourceLimiter(
            max_concurrency=apollo_params["max_concurrency"],
            qps=apollo_params["qps"]
        ),
        event_bus=event_bus,
        startup_priority=0
    )

    # 将 Apollo 资源添加到工厂的资源管理器中
    app_factory.add_resource(apollo)


def _auto_wire_modules(app_factory: FastChainApp) -> None:
    """动态模块装配（Phase 4: Auto-Wiring）

    根据配置中心的模块列表，动态发现并装配业务资源模块（如 LLM、数据库、提示词管理器等）以及 API 路由模块

    Args:
        app_factory (FastChainApp): 应用工厂实例,提供自动发现和装配能力（auto_discover_resources、auto_discover_routers 方法）
    """
    settings = app_factory.settings

    # 从配置中心读取启用的资源模块列表
    # 若配置缺失或格式错误，get_enabled_resource_modules 会抛出异常（Strict Mode）
    resource_modules = bootstrap.get_enabled_resource_modules(settings)

    # 从配置中心读取启用的路由模块列表
    # 若配置缺失或格式错误，get_enabled_router_modules 会抛出异常（Strict Mode）
    router_modules = bootstrap.get_enabled_router_modules(settings)

    # 链式调用（Fluent API 风格）：先发现资源模块，再发现 API 路由模块
    (
        app_factory
        .auto_discover_resources(resource_modules)
        .auto_discover_routers(router_modules)
    )


def create_app() -> FastAPI:
    """FastChain 应用工厂入口（五阶段流水线式启动流程）

    采用声明式流程编排（Pipeline Style），按以下五个阶段依次初始化应用：

    Phase 1: Infrastructure（基础设施）
    - 创建应用工厂实例，加载版本号和默认配置
    - 职责：提供后续阶段所需的基础对象（如 settings、event_bus）

    Phase 2: Bootstrap（配置引导，关键路径）
    - 从远程配置中心（Apollo）拉取配置，或在网络不可用时回退到本地缓存
    - 职责：确保应用拥有完整的配置，采用 Fail-fast 策略（失败即退出）
    - 关键路径：此阶段失败会导致整个应用无法启动

    Phase 3: Core Resources（核心资源）
    - 注册核心资源（如 Apollo 客户端），确保后续轮询和动态配置更新能力
    - 职责：提供配置热更新、事件驱动等基础能力

    Phase 4: Auto-Wiring（动态装配）
    - 根据配置启用的模块列表，自动发现并装配业务资源（如 LLM、数据库）和 API 路由
    - 职责：将静态的代码模块转化为运行时的资源和路由实例

    Phase 5: Business Hooks（业务钩子）
    - 在应用启动时执行依赖注入和服务装配（如 ChatService、监听器等）
    - 职责：将已初始化的资源组装成可用的业务服务，并挂载到应用的全局状态中

    Returns:
        FastAPI: 完全配置好的 FastAPI 应用实例，可直接用于 ASGI 服务器（如 Uvicorn）启动

    Raises:
        RuntimeError: 当配置引导（Phase 2）或模块装配（Phase 4）失败时抛出，确保应用不会以不完整状态启动

    Note:
        - 各阶段严格按顺序执行，前一阶段失败则后续阶段不会执行（Fail-fast）
        - 关键路径（配置引导）采用 Fail-fast 策略：若配置拉取完全失败（网络和缓存均不可用），应用将立即退出
        - 业务钩子（wiring.wire_services）会在 FastAPI 的 on_startup 事件中被调用，确保在应用接收请求之前完成服务装配
    """

    # Phase 1: 基础设施
    # 此阶段仅初始化最基本的对象，不涉及任何外部依赖或网络 I/O
    app_factory = _init_infrastructure()

    # Phase 2: 配置引导（Bootstrap，关键路径）
    # 从 Apollo 配置中心拉取配置或使用本地缓存
    # 若拉取失败（网络和缓存均不可用），应用将抛出 RuntimeError 并退出
    _bootstrap_configuration(app_factory)

    # Phase 3: 核心资源
    # 注册 Apollo 资源，确保后续轮询和动态配置更新能力
    # Apollo 资源的启动优先级设为 0（最高），确保在所有业务资源之前启动
    _register_core_resources(app_factory)

    # Phase 4: 动态装配
    # 根据配置启用的模块列表，自动发现并装配业务资源和 API 路由
    _auto_wire_modules(app_factory)

    # Phase 5: 业务钩子
    # 在应用启动时执行依赖注入和服务装配（如 ChatService、监听器等）
    # wiring.wire_services 会在 FastAPI 的 on_startup 事件中被调用，
    # 负责将已初始化的底层资源组装成可用的业务服务实例，并挂载到应用的全局状态中
    app_factory.on_startup(wiring.wire_services)

    return app_factory.build()
