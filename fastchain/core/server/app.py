from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import pkgutil
from contextlib import asynccontextmanager
from types import ModuleType
from typing import List, Optional, Any, Set

from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from prometheus_client import make_asgi_app

from ...core.config import create_settings_manager, SettingsManager
from ...core.config.constants import AppConstants
from ...core.logging import create_app_logger
from ...core.resources.base import ResourceManager, Resource
from ...core.resources.events import EventBus, EventConstants
from ...core.server.middleware import PrometheusMiddleware, TraceIDMiddleware


async def _log_lifecycle_event(event: Any) -> None:
    """记录资源生命周期事件的回调函数

    订阅资源启动、停止、启动失败等事件，记录到日志

    Args:
        event (Any): 事件对象（包含 type 和 payload）

    Note:
        使用 await asyncio.sleep(0) 触发事件循环调度，
        确保在高并发场景下事件处理不会阻塞其他协程
    """
    # 协程调度点，避免阻塞
    await asyncio.sleep(0)
    logger.bind(event_type=event.type).info(f"Event: {event.payload}")


async def _log_config_update(event: Any) -> None:
    """记录配置更新事件的回调函数

    订阅配置中心推送的配置更新事件，记录更新的命名空间

    Args:
        event (Any): 配置更新事件对象

    Note:
        配置更新事件是分布式系统中的关键事件，记录命名空间信息有助于追踪配置变更和排查问题
    """
    # 协程调度点
    await asyncio.sleep(0)
    updated_ns = event.payload.get(EventConstants.KEY_UPDATED_NAMESPACES)
    logger.bind(event_type=event.type).success(f"配置已更新: {updated_ns}")


class FastChainApp:
    """FastChain 应用核心工厂类

    核心职责：
    - 聚合配置管理（本地配置 + 运行时配置 + 路径配置）
    - 自动发现和装配资源（Resource）和路由（APIRouter）
    - 管理应用生命周期（启动、停止、事件订阅）
    - 构建并配置 FastAPI 应用实例（中间件、CORS、指标暴露等）

    设计理念：
    - 配置驱动：所有模块启用、中间件配置均通过 Apollo 配置中心控制
    - 约定优于配置：遵循命名约定（如路由模块导出 `router` 变量）自动发现
    - 依赖注入：通过 app.state 注入 SettingsManager、ResourceManager、EventBus
    - 生命周期管理：使用 FastAPI lifespan 机制协调资源启动顺序和优雅关闭

    Attributes:
        app_name (str): 应用名称
        version (str): 应用版本号
        settings (SettingsManager): 配置管理器
        event_bus (EventBus): 事件总线
        resource_manager (ResourceManager): 资源管理器
        _fastapi_app (FastAPI | None): FastAPI 应用实例（延迟构建）
        _resources (List[Resource]): 已注册的资源实例列表
        _routers (List[APIRouter]): 已发现的路由器列表
        _on_startup_hooks (List): 启动钩子函数列表
        _loaded_resource_classes (Set[Any]): 已加载的资源类集合（防重复注册）

    Methods:
        add_resource: 手动添加资源实例
        on_startup: 注册启动钩子
        auto_discover_resources: 自动发现并注册资源
        auto_discover_routers: 自动发现并注册路由
        build: 构建并返回配置完成的 FastAPI 应用实例
    """

    def __init__(
            self,
            app_name: str | None = None,
            version: str | None = None
    ) -> None:
        """初始化 FastChain 应用工厂

        Args:
            app_name (str | None): 应用名称默认为 None（使用默认名称）
            version (str | None): 应用版本号默认为 None（使用默认版本）

        Note:
            配置转换器注册必须在任何资源加载或配置拉取之前完成，
            确保所有模块在读取配置时拿到的都是已转换的对象而非原始字符串
        """
        # 创建配置管理器（聚合本地配置、运行时配置、路径配置）
        self.settings: SettingsManager = create_settings_manager()

        # 确定应用名称：优先级 app_name > AppConstants > 项目名称
        self.app_name = (
                app_name
                or AppConstants.DEFAULT_APP_NAME
                or self.settings.paths.project_name
        )
        self.version = version or AppConstants.DEFAULT_VERSION

        # 初始化日志系统（基于配置管理器）
        create_app_logger(self.settings, app_name=self.app_name)

        # 注册默认配置转换器，在配置拉取后自动将 JSON 字符串解析为 Python 对象
        # 免去各业务模块重复解析的麻烦，实现配置解析的集中管理
        self._register_default_transformers()

        # 创建事件总线和资源管理器
        self.event_bus = EventBus()
        self.resource_manager = ResourceManager(event_bus=self.event_bus)

        # 延迟创建的 FastAPI 应用实例
        self._fastapi_app: Optional[FastAPI] = None

        # 资源和路由注册表
        self._resources: List[Resource] = []
        self._routers: List[APIRouter] = []

        # 启动钩子函数列表
        self._on_startup_hooks = []

        # 已加载资源类集合（防止重复注册）
        self._loaded_resource_classes: Set[Any] = set()

    def _register_default_transformers(self) -> None:
        """注册默认的配置转换规则

        自动将 Apollo 中的特定 JSON 字符串配置转换为 Python 字典，免去各业务模块重复解析的麻烦

        注册的转换器：
        - system.config: 核心系统配置（资源模块、路由模块、安全配置等）
        - jobs.config: 任务调度配置（定时任务列表、Cron 表达式等）
        - llm.models: 模型参数配置（LLM 模型列表、默认参数等）

        Note:
            转换器的执行时机是在配置存入 RuntimeConfigStore 之前，
            而非每次读取时转换，确保转换只发生一次，性能最优
            如果后续需要新增配置节点，只需在此方法中添加对应的注册逻辑即可
        """
        ns = self.settings.local.apollo_namespace

        # 1. 注册 system.config (核心系统配置)
        # Apollo 中存储格式：key="system.config", value="{\"resources\":{...},\"routers\":{...}}"
        # 转换后格式：key="system.config", value={"resources":{...},"routers":{...}}
        self.settings.register_transformer(ns, "system.config", json.loads)

        # 2. 注册 jobs.config (任务调度配置)
        # Apollo 中存储格式：key="jobs.config", value="{\"schedules\":{...}}"
        # 转换后格式：key="jobs.config", value={"schedules":{...}}
        self.settings.register_transformer(ns, "jobs.config", json.loads)

        # 3. 注册 llm.models (模型参数配置)
        # Apollo 中存储格式：key="llm.models", value="[{\"name\":\"gpt-4\",...}]"
        # 转换后格式：key="llm.models", value=[{"name":"gpt-4",...}]
        self.settings.register_transformer(ns, "llm.models", json.loads)

        logger.debug("已注册默认配置转换规则 (system.config, jobs.config, llm.models)")

    def add_resource(self, resource: Resource) -> "FastChainApp":
        """手动添加资源实例到应用

        Args:
            resource (Resource): 资源实例（必须继承自 Resource 基类）

        Returns:
            FastChainApp: 当前应用实例（支持链式调用）

        Note:
            手动添加的资源优先级高于自动发现的资源，
            适用于需要自定义初始化参数或特殊配置的场景
        """
        self._resources.append(resource)
        self._loaded_resource_classes.add(resource.__class__)
        return self

    def on_startup(self, func) -> "FastChainApp":
        """注册应用启动钩子函数

        启动钩子在所有资源启动完成后、应用接收请求前执行，
        适用于需要访问已启动资源的初始化逻辑（如数据预热、健康检查等）

        Args:
            func: 启动钩子函数（可以是同步或异步函数）

        Returns:
            FastChainApp: 当前应用实例（支持链式调用）

        Note:
            钩子函数可以接收 FastAPI app 实例作为参数（可选），用于访问 app.state 中的资源管理器等对象
        """
        self._on_startup_hooks.append(func)
        return self

    def auto_discover_resources(self, root_module_names: List[str]) -> "FastChainApp":
        """自动发现并注册资源

        递归扫描指定模块树，查找所有继承自 Resource 的类，并自动实例化和注册到资源管理器

        Args:
            root_module_names (List[str]): 资源根模块路径列表（如 ["app.resources"]）

        Returns:
            FastChainApp: 当前应用实例（支持链式调用）

        Note:
            自动发现机制基于以下约定：
            - 资源类必须继承自 Resource 基类
            - 资源类的构造函数签名必须为 __init__(self, settings, event_bus)
            - 资源类必须定义在指定的根模块树中
        """
        for root_name in root_module_names:
            logger.info(f"🔍 正在自动发现资源 '{root_name}'...")
            try:
                # 导入根模块
                root_module = importlib.import_module(root_name)

                # 递归扫描根模块及其子模块
                self._recursive_scan_resources(root_module)
            except ModuleNotFoundError:
                logger.warning(f"资源根模块 '{root_name}' 未找到，跳过")
            except ImportError as e:
                logger.error(f"导入资源根模块 '{root_name}' 失败: {e}")
        return self

    def _recursive_scan_resources(self, module: ModuleType) -> None:
        """递归扫描模块中的资源类

        Args:
            module (ModuleType): Python 模块对象

        Note:
            递归终止条件：模块没有 __path__ 属性（即叶子模块，非包）
            使用 pkgutil.iter_modules 遍历子模块，性能优于 os.listdir
        """
        # 如果模块没有 __path__ 属性，说明不是包（是普通模块），停止递归
        if not hasattr(module, "__path__"):
            return

        # 遍历模块中的所有子模块
        for _, name, is_pkg in pkgutil.iter_modules(module.__path__):
            full_name = f"{module.__name__}.{name}"
            try:
                # 导入子模块
                sub_module = importlib.import_module(full_name)

                # 如果是包，递归扫描
                if is_pkg:
                    self._recursive_scan_resources(sub_module)

                # 检查并注册资源类
                self._inspect_and_register_resource(sub_module)
            except Exception as e:
                logger.warning(f"扫描模块 {full_name} 时出错: {e}")

    def _inspect_and_register_resource(self, module: ModuleType) -> None:
        """检查模块中的 Resource 子类并注册

        遍历模块的所有成员，查找符合条件的 Resource 子类，并尝试实例化和注册

        Args:
            module (ModuleType): Python 模块对象

        Note:
            实例化失败时（如签名不匹配），会记录 debug 日志而非错误日志，
            因为某些抽象类或工具类可能不需要实例化
        """
        for name, obj in inspect.getmembers(module):
            # 过滤出可注册的资源类
            if not self._is_registrable_resource(obj, module):
                continue

            try:
                # 尝试实例化资源类（自动注入 settings 和 event_bus）
                instance = obj(settings=self.settings, event_bus=self.event_bus)
                logger.info(f"🧩 自动装配资源: {obj.__name__}")
                self.add_resource(instance)
            except TypeError as e:
                # 签名不匹配（可能是个抽象类或工具类），记录 debug 日志
                logger.debug(f"跳过资源 {name}：实例化失败（签名不匹配？）: {e}")
            except Exception as e:
                # 其他异常，记录错误日志
                logger.error(f"实例化资源 {name} 失败: {e}")

    def _is_registrable_resource(self, obj: Any, module: ModuleType) -> bool:
        """判断对象是否为可注册的资源类

        过滤条件：
        - 必须是类
        - 必须是 Resource 的子类
        - 不能是 Resource 基类本身
        - 不能是已注册的类（防止重复注册）
        - 必须定义在当前模块中（防止注册导入的外部类）

        Args:
            obj (Any): 待检查的对象
            module (ModuleType): 当前扫描的模块

        Returns:
            bool: 如果可注册返回 True，否则返回 False

        Note:
            检查 obj.__module__ == module.__name__ 是为了防止注册从其他模块导入的类，
            这在复杂项目中非常重要，避免同一个资源被重复注册多次
        """
        # 检查是否为类
        if not inspect.isclass(obj):
            return False

        # 检查是否为 Resource 子类
        if not issubclass(obj, Resource):
            return False

        # 排除 Resource 基类本身
        if obj is Resource:
            return False

        # 排除已注册的类
        if obj in self._loaded_resource_classes:
            return False

        # 排除从其他模块导入的类
        if obj.__module__ != module.__name__:
            return False

        return True

    def auto_discover_routers(self, root_module_names: List[str]) -> "FastChainApp":
        """自动发现并注册路由

        递归扫描指定模块树，查找所有导出 `router` 变量的模块，并自动注册到 FastAPI 应用

        Args:
            root_module_names (List[str]): 路由根模块路径列表（如 ["app.api"]）

        Returns:
            FastChainApp: 当前应用实例（支持链式调用）

        Note:
            自动发现机制基于约定：路由模块必须导出名为 `router` 的 APIRouter 实例
        """
        for root_name in root_module_names:
            logger.info(f"🔍 正在自动发现路由 '{root_name}'...")
            try:
                # 导入根模块
                root_module = importlib.import_module(root_name)

                # 递归扫描根模块及其子模块
                self._recursive_scan_routers(root_module)
            except ImportError as e:
                logger.error(f"导入路由根模块 '{root_name}' 失败: {e}")
        return self

    def _recursive_scan_routers(self, module: ModuleType) -> None:
        """递归扫描模块中的路由器

        Args:
            module (ModuleType): Python 模块对象

        Note:
            与资源扫描类似，递归终止条件是模块没有 __path__ 属性
        """
        # 如果模块没有 __path__ 属性，说明不是包，停止递归
        if not hasattr(module, "__path__"):
            return

        # 遍历模块中的所有子模块
        for _, name, is_pkg in pkgutil.iter_modules(module.__path__):
            full_name = f"{module.__name__}.{name}"
            try:
                # 导入子模块
                sub_module = importlib.import_module(full_name)

                # 如果是包，递归扫描
                if is_pkg:
                    self._recursive_scan_routers(sub_module)
                else:
                    # 如果是模块，检查是否导出 `router` 变量
                    router_obj = getattr(sub_module, "router", None)
                    if isinstance(router_obj, APIRouter):
                        logger.debug(f"在模块中找到路由: {full_name}")
                        self._routers.append(router_obj)
            except Exception as e:
                logger.warning(f"扫描模块 {full_name} 时出错: {e}")

    def _setup_lifespan(self):
        """设置应用生命周期管理器

        使用 FastAPI 的 lifespan 机制协调资源启动、停止和事件订阅

        工作流程：
        1. 订阅资源生命周期事件和配置更新事件
        2. 启动事件总线
        3. 注册所有资源到资源管理器
        4. 将核心对象注入到 app.state（供路由和中间件访问）
        5. 按优先级启动所有资源
        6. 执行用户注册的启动钩子
        7. 应用运行期间 yield（等待关闭信号）
        8. 按逆序停止所有资源
        9. 停止事件总线

        Returns:
            AsyncContextManager: 异步上下文管理器（lifespan 回调）

        Note:
            使用 @asynccontextmanager 装饰器简化生命周期管理，
            避免手动处理 startup/shutdown 事件
        """

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """应用生命周期上下文管理器

            Args:
                app (FastAPI): FastAPI 应用实例

            Yields:
                None: 在应用启动完成后 yield，等待关闭信号

            Raises:
                Exception: 如果启动过程中发生错误

            Note:
                启动钩子的执行顺序在资源启动之后，确保钩子可以访问已启动的资源
                钩子执行失败会导致整个应用启动失败，这是有意设计（fail-fast）
            """
            logger.info(f"🚀 正在启动 {self.app_name} v{self.version}...")

            # 订阅资源生命周期事件
            for event_type in (
                    EventConstants.RESOURCE_STARTED,
                    EventConstants.RESOURCE_STOPPED,
                    EventConstants.RESOURCE_START_FAILED,
            ):
                self.event_bus.subscribe(event_type, _log_lifecycle_event)

            # 订阅配置更新事件
            self.event_bus.subscribe(EventConstants.CONFIG_UPDATED, _log_config_update)

            # 启动事件总线
            await self.event_bus.start()

            # 注册所有资源到资源管理器
            for res in self._resources:
                self.resource_manager.register(res)

            # 将核心对象注入到 app.state（供路由和中间件访问）
            app.state.settings = self.settings
            app.state.resource_manager = self.resource_manager
            app.state.event_bus = self.event_bus

            try:
                # 按优先级启动所有资源
                await self.resource_manager.start_all()

                # 执行用户注册的启动钩子
                for hook in self._on_startup_hooks:
                    try:
                        # 自动识别同步/异步钩子
                        if asyncio.iscoroutinefunction(hook):
                            await hook(app)
                        else:
                            hook(app)
                    except Exception as e:
                        logger.critical(f"启动钩子 '{hook.__name__}' 失败: {e}")
                        raise e

                logger.success("✅ 系统就绪")

                # 控制权，等待应用关闭信号
                yield

            except Exception as e:
                logger.critical(f"❌ 启动失败: {e}")
                raise
            finally:
                # 应用关闭流程
                logger.info("🛑 正在关闭...")

                # 按逆序停止所有资源
                await self.resource_manager.stop_all()

                # 停止事件总线
                await self.event_bus.stop()

                logger.info("👋 拜拜.")

        return lifespan

    def _setup_cors(self, app: FastAPI) -> None:
        """配置并应用 CORS (跨域资源共享) 中间件

        从运行时配置中读取 CORS 策略，动态配置跨域访问控制

        Args:
            app (FastAPI): FastAPI 应用实例

        Note:
            由于在 _register_default_transformers 中已注册配置转换器，
            这里 get_value 返回的 system.config 一定是 dict 对象，
            无需手动 JSON 解析，大幅简化了配置加载逻辑
            CORS 配置支持热更新（配置更新后重启应用生效）
        """
        ns = self.settings.local.apollo_namespace

        try:
            # 获取已转换的系统配置（转换器已完成 JSON 解析）
            sys_conf = self.settings.get_value(ns, "system.config", default={})

            # 防御性检查：确保转换器正确工作
            if not isinstance(sys_conf, dict):
                return

            # 提取安全配置和 CORS 配置
            security_conf = sys_conf.get("security", {})
            cors_conf = security_conf.get("cors", {})

            # 检查 CORS 是否启用
            if not cors_conf.get("enabled", False):
                logger.debug("CORS 配置未启用或缺失，跳过中间件注册")
                return

            logger.info("🛡️ 正在应用 CORS 跨域策略...")

            # 注册 CORS 中间件
            app.add_middleware(
                CORSMiddleware,
                # 允许的来源列表
                allow_origins=cors_conf.get("allow_origins", []),
                # 是否允许携带凭证
                allow_credentials=cors_conf.get("allow_credentials", False),
                # 允许的 HTTP 方法
                allow_methods=cors_conf.get("allow_methods", ["GET"]),
                # 允许的请求头
                allow_headers=cors_conf.get("allow_headers", []),
                # 预检请求缓存时间（秒）
                max_age=cors_conf.get("max_age", 600),
            )
            logger.success(f"CORS 中间件已启用允许源: {cors_conf.get('allow_origins')}")

        except Exception as e:
            logger.error(f"应用 CORS 配置时出错: {e}")

    def build(self) -> FastAPI:
        """构建并返回配置完成的 FastAPI 应用实例

        执行流程：
        1. 创建 FastAPI 应用实例（配置 title、version、lifespan）
        2. 挂载 Prometheus 指标暴露端点
        3. 注册所有自动发现的路由器
        4. 注册中间件（Prometheus、TraceID、CORS）

        Returns:
            FastAPI: 配置完成的 FastAPI 应用实例

        Note:
            中间件的注册顺序很重要：先注册的中间件在请求处理链的外层，
            因此应该先注册 PrometheusMiddleware（用于指标收集），
            最后注册 CORS（用于跨域控制）
        """
        # 创建 FastAPI 应用实例
        self._fastapi_app = FastAPI(
            title=self.app_name,
            version=self.version,
            # 注入生命周期管理器
            lifespan=self._setup_lifespan()
        )

        # 挂载 Prometheus 指标暴露端点
        # 使用 ASGI 应用实例，支持异步指标收集
        metrics_app = make_asgi_app()
        self._fastapi_app.mount("/metrics", metrics_app)

        # 注册所有自动发现的路由器
        for router in self._routers:
            self._fastapi_app.include_router(router)

        # 注册中间件（注册顺序决定执行顺序）
        # 1. PrometheusMiddleware: 收集请求指标（响应时间、状态码等）
        self._fastapi_app.add_middleware(PrometheusMiddleware)

        # 2. TraceIDMiddleware: 为每个请求生成唯一追踪 ID（用于日志关联和分布式追踪）
        self._fastapi_app.add_middleware(TraceIDMiddleware)

        # 3. CORS: 配置跨域访问策略（最外层，最先执行）
        self._setup_cors(self._fastapi_app)

        return self._fastapi_app
    