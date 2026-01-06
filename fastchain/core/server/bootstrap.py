from __future__ import annotations

import asyncio
import importlib.metadata
import threading
from typing import Any, Dict, List, Type, TypeVar

from loguru import logger

from ..config.constants import AppConstants
from ..config.settings_manager import SettingsManager
from ..resources.apollo import AsyncApolloClient

T = TypeVar("T")


def _get_installed_version(candidates: tuple[str, ...]) -> str:
    """从已安装的包元数据中读取版本号（兼容多个发行名）

    Args:
        candidates (tuple[str, ...]): 发行包名候选列表（按优先级排序）

    Returns:
        str: 读取到的版本号（若均未安装则返回默认版本号）
    """
    for dist_name in candidates:
        try:
            return importlib.metadata.version(dist_name)
        except importlib.metadata.PackageNotFoundError:
            continue
    return AppConstants.DEFAULT_VERSION


def get_app_version() -> str:
    """获取应用的版本号

    尝试从已安装的包元数据中读取版本号，如果读取失败（如开发模式下未安装包），则返回默认版本号

    Returns:
        str: 应用版本号

    Note:
        使用 importlib.metadata 是 Python 3.8+ 推荐的包元数据读取方式，比传统的 pkg_resources 性能更好且更符合现代 Python 规范
    """
    try:
        # 从已安装的包元数据中读取版本号
        return _get_installed_version(("fastchain-cucc", "fastchain"))
    except importlib.metadata.PackageNotFoundError:
        # 包未安装时（如开发模式），返回默认版本号
        return AppConstants.DEFAULT_VERSION


def load_apollo_params(settings: SettingsManager) -> Dict[str, Any]:
    """安全加载 Apollo 配置中心的引导参数

    从运行时配置中提取 Apollo 客户端的启动参数（轮询间隔、QPS、最大并发数等），并提供类型转换和容错机制

    Args:
        settings (SettingsManager): 配置管理器实例

    Returns:
        Dict[str, Any]: Apollo 客户端参数字典，包含以下键：
            - poll_interval (float): 配置轮询间隔（秒）默认 10.0
            - qps (float): 限流 QPS默认 2.0
            - max_concurrency (int): 最大并发请求数默认 5

    Note:
        Apollo 配置节点通常是散列的 KV 对，而非单个 JSON 字符串
        如果你的 Apollo 配置把 apollo 参数也存成了 JSON 字符串，
        需要在 app.py 中为该节点注册转换器
    """
    ns = settings.local.apollo_namespace

    # 直接从运行时配置中获取 apollo 节点（已通过转换器解析）
    apollo_conf = settings.get_value(ns, "apollo", default={})

    # 防御性检查：确保返回的是字典
    if not isinstance(apollo_conf, dict):
        apollo_conf = {}

    def safe_get(key: str, type_func: Type[T], default: T) -> T:
        """安全获取并转换配置值

        Args:
            key (str): 配置键名
            type_func (Type[T]): 目标类型转换函数
            default (T): 默认值

        Returns:
            T: 转换后的配置值，失败时返回默认值

        Note:
            使用 try-except 包裹类型转换，防止因配置类型错误导致启动失败
            这是配置容错的最佳实践：宽进严出，但绝不因为配置问题导致系统崩溃
        """
        val = apollo_conf.get(key)
        try:
            return type_func(val) if val is not None else default
        except (ValueError, TypeError):
            logger.warning(
                f"Apollo 配置项 '{key}' 无效期望类型 {type_func.__name__}，实际获得 {type(val)}"
                f" 使用默认值: {default}"
            )
            return default

    return {
        # 配置轮询间隔：决定 Apollo 客户端多久拉取一次配置更新
        "poll_interval": safe_get("poll_interval", float, 10.0),
        # 限流 QPS：防止配置拉取过于频繁导致 Apollo 服务端压力过大
        "qps": safe_get("qps", float, 2.0),
        # 最大并发数：限制同时进行的配置拉取请求数量
        "max_concurrency": safe_get("max_concurrency", int, 5)
    }


def _load_system_config(settings: SettingsManager) -> Dict[str, Any]:
    """加载 system.config 配置节点

    从运行时配置中提取核心系统配置（如资源模块列表、路由模块列表等）

    Args:
        settings (SettingsManager): 配置管理器实例

    Returns:
        Dict[str, Any]: 系统配置字典

    Raises:
        ValueError: 如果配置缺失或类型错误

    Note:
        由于在 app.py 的 _register_default_transformers 中已经为 system.config
        注册了 json.loads 转换器，这里 get_value 返回的一定是 Python dict 对象，
        无需手动解析 JSON 字符串这大幅简化了配置加载逻辑
    """
    ns = settings.local.apollo_namespace

    # 直接获取已转换的字典对象（转换器已完成 JSON 解析）
    sys_conf = settings.get_value(ns, "system.config")

    if not sys_conf:
        raise ValueError(f"配置 'system.config' 在命名空间 '{ns}' 中缺失")

    # 防御性检查：确保转换器正确工作
    # 如果转换器配置错误或 Apollo 存储的不是 JSON 字符串，这里会拿到错误的类型
    if not isinstance(sys_conf, dict):
        raise ValueError(f"'system.config' 必须是一个 JSON 对象 (dict)，实际类型: {type(sys_conf)}")

    return sys_conf


def get_enabled_resource_modules(settings: SettingsManager) -> List[str]:
    """获取需要自动装配的资源模块列表

    从 system.config.resources.modules 中读取资源模块列表，用于应用启动时的自动发现和装配

    Args:
        settings (SettingsManager): 配置管理器实例

    Returns:
        List[str]: 资源模块路径列表（如 ["app.resources.db", "app.resources.cache"]）

    Raises:
        RuntimeError: 如果配置加载失败或模块列表无效

    Note:
        资源模块自动发现机制允许通过配置控制应用启动时加载哪些组件，实现松耦合和灵活部署（如测试环境可以禁用某些重量级资源）
    """
    try:
        sys_conf = _load_system_config(settings)
        modules = sys_conf.get("resources", {}).get("modules")

        # 校验模块列表：必须是非空列表
        if not isinstance(modules, list) or not modules:
            raise ValueError(
                "'resources.modules' 列表缺失或无效"
                "请在 Apollo 中配置启用的模块"
            )

        logger.info(f"使用配置的资源模块: {len(modules)} 项")
        return [str(m) for m in modules]

    except Exception as e:
        logger.critical(f"加载资源配置失败: {e}")
        raise RuntimeError(f"资源模块配置失败: {e}") from e


def get_enabled_router_modules(settings: SettingsManager) -> List[str]:
    """获取需要自动装配的路由模块列表

    从 system.config.routers.modules 中读取路由模块列表，用于应用启动时的自动发现和装配

    Args:
        settings (SettingsManager): 配置管理器实例

    Returns:
        List[str]: 路由模块路径列表（如 ["app.api.v1", "app.api.v2"]）

    Raises:
        RuntimeError: 如果配置加载失败或模块列表无效

    Note:
        路由模块自动发现机制允许通过配置控制 API 版本和功能模块的启用状态，
        实现灵活的 API 管理和版本迭代
    """
    try:
        sys_conf = _load_system_config(settings)
        modules = sys_conf.get("routers", {}).get("modules")

        # 校验模块列表：必须是非空列表
        if not isinstance(modules, list) or not modules:
            raise ValueError(
                "'routers.modules' 列表缺失或无效"
                "请在 Apollo 中配置启用的路由模块"
            )

        logger.info(f"使用配置的路由模块: {len(modules)} 项")
        return [str(m) for m in modules]

    except Exception as e:
        logger.critical(f"加载路由配置失败: {e}")
        raise RuntimeError(f"路由模块配置失败: {e}") from e


async def _bootstrap_config_logic(settings: SettingsManager) -> None:
    """执行配置拉取的核心逻辑

    工作流程：
    1. 尝试从 Apollo 服务端拉取配置
    2. 如果网络拉取失败，尝试从本地缓存加载
    3. 如果两者都失败，抛出异常（应用无法启动）

    Args:
        settings (SettingsManager): 配置管理器实例

    Raises:
        RuntimeError: 如果无法从任何来源获取配置

    Note:
        配置引导是应用启动的第一步，失败时应立即终止启动流程，
        而非使用空配置或默认配置继续运行（这会导致难以排查的运行时错误）
        网络拉取失败后自动降级到本地缓存，实现配置的离线容错
    """
    logger.info("🔥 正在拉取启动阶段配置...")

    local = settings.local
    target_namespaces = [local.apollo_namespace]
    cache_dir = settings.paths.data_dir / "apollo_cache"

    # 创建 Apollo 客户端实例
    client = AsyncApolloClient(
        server_url=str(local.apollo_server_url),
        app_id=local.apollo_app_id,
        cluster=local.apollo_cluster,
        namespaces=target_namespaces,
        # 如果需要 secret，从 local 配置中读取
        secret=None,
        # 网络请求超时时间（秒）
        timeout=5.0,
        cache_dir=cache_dir
    )

    # 第一步：尝试从网络拉取配置
    try:
        raw_sections = await client.fetch_all()
        if raw_sections:
            # 将配置键规范化为字符串（防止枚举类型导致的不一致）
            sections = {str(k): v for k, v in raw_sections.items()}

            # 更新运行时配置存储
            await settings.runtime_store.update_sections(sections, source="bootstrap_net")

            # 保存到本地缓存（用于下次离线启动）
            await client.save_cache(sections)

            logger.success("配置已从网络引导加载")
            return
    except Exception as e:
        logger.warning(f"启动阶段网络获取失败: {e}")

    # 第二步：尝试从本地缓存加载配置
    try:
        cached_sections = await client.load_cache()
        if cached_sections:
            await settings.runtime_store.update_sections(cached_sections, source="bootstrap_cache")
            logger.warning("配置已从本地缓存引导加载（离线模式）")
            return
    except Exception as e:
        logger.error(f"启动阶段缓存加载失败: {e}")

    # 第三步：两者都失败，抛出异常
    error_msg = "严重错误：无法从任何来源（网络和缓存）引导配置"
    logger.error(error_msg)
    raise RuntimeError(error_msg)


def run_bootstrap_in_thread(settings: SettingsManager) -> None:
    """在独立线程中运行配置引导协程

    由于 FastAPI 应用启动前可能还没有主事件循环，需要在独立线程中创建临时事件循环来执行 async 配置拉取

    Args:
        settings (SettingsManager): 配置管理器实例

    Raises:
        Exception: 如果配置引导失败，将异常传播到主线程

    Note:
        使用独立线程而非主线程执行，是因为应用启动流程可能是同步的（如 Gunicorn pre-fork 模式）
        线程中的事件循环在完成后会被正确清理（shutdown_asyncgens + close），防止资源泄漏和后续事件循环冲突
    """
    exception_bucket: List[Exception] = []

    def runner():
        """线程执行器函数

        创建新的事件循环，执行配置引导逻辑，并捕获异常
        """
        # 创建新的事件循环（线程隔离）
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # 在新事件循环中执行配置引导
            loop.run_until_complete(_bootstrap_config_logic(settings))
        except Exception as e:
            # 捕获异常，存储到外部容器中（用于主线程检查）
            exception_bucket.append(e)
        finally:
            # 清理异步生成器（防止资源泄漏）
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                # 关闭事件循环
                loop.close()

    # 创建并启动线程
    t = threading.Thread(target=runner, name="bootstrap_thread")
    t.start()

    # 等待线程完成
    t.join()

    # 如果线程中发生异常，重新抛出到主线程
    if exception_bucket:
        raise exception_bucket[0]
    