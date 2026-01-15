from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Optional, Set, Any, Dict, override

import elasticsearch
from elasticsearch import AsyncElasticsearch
from loguru import logger
from pydantic import ValidationError

from .config import ElasticsearchConfig
from .....core.config import SettingsManager
from .....core.config.constants import ResourceKind
from .....core.resources.base import Resource, ResourceHealth, HealthStatus
from .....core.resources.events import Event, EventBus, EventConstants
from .....core.resources.watcher import BackgroundWatcher


class ElasticsearchHealthWatcher(BackgroundWatcher):
    """Elasticsearch 主动健康检查观察者

    基于 BackgroundWatcher 调度机制，定期对 ES 集群执行轻量级健康探测

    Attributes:
        _manager (ElasticsearchManager): 关联的 ES 资源管理器
    """

    # 健康检查等待状态阈值与超时
    HEALTH_WAIT_STATUS = "yellow"
    HEALTH_TIMEOUT = "5s"

    def __init__(
        self,
        manager: ElasticsearchManager,
        event_bus: EventBus | None = None
    ):
        """初始化健康检查观察者

        Args:
            manager: 关联的 ElasticsearchManager 实例
            event_bus: 可选的事件总线，用于发布内部事件
        """
        super().__init__(
            name=f"{manager.name}-health",
            logger=logger,
            min_interval=60.0,
            max_interval=120.0,
            event_bus=event_bus
        )
        self._manager = manager

    @override
    async def tick(self) -> None:
        """执行单次健康检查

        调用 ES cluster.health API 进行轻量级探测，若客户端不可用则静默跳过
        """
        client = self._manager.get_active_client()
        if not client:
            return

        await client.cluster.health(
            wait_for_status=self.HEALTH_WAIT_STATUS,
            timeout=self.HEALTH_TIMEOUT
        )


class ElasticsearchManager(Resource):
    """Elasticsearch 资源管理器

    遵循 FastChain 标准资源模式，支持配置热重载与优雅关闭

    Attributes:
        _client (AsyncElasticsearch | None): 当前活跃的 ES 异步客户端
        _config (ElasticsearchConfig | None): 当前生效的配置
        _started (bool): 启动状态标志，用于事件回调守卫

    Methods:
        start: 启动资源，订阅配置事件并建立连接
        stop: 停止资源，清理后台任务与连接
        reload: 热重载配置并重建连接
        health_check: 返回当前健康状态
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus):
        """初始化 Elasticsearch 资源管理器

        Args:
            settings: 全局配置管理器
            event_bus: 事件总线，用于订阅配置变更
        """
        super().__init__(
            name="elasticsearch",
            kind=ResourceKind.VECTOR_STORE,
            startup_priority=10
        )
        self._settings = settings
        self._event_bus = event_bus

        self._client: AsyncElasticsearch | None = None
        self._config: ElasticsearchConfig | None = None

        self._reload_lock = asyncio.Lock()
        self._started = False

        # 管理异步关闭旧连接的后台任务，避免阻塞主流程
        self._background_tasks: Set[asyncio.Task] = set()

        self._health_watcher = ElasticsearchHealthWatcher(self, event_bus)

    @property
    def client(self) -> AsyncElasticsearch:
        """获取 ES 客户端（严格模式）

        Returns:
            AsyncElasticsearch: 当前活跃的客户端实例

        Raises:
            RuntimeError: 若管理器未启动或配置无效
        """
        if not self._client:
            raise RuntimeError("ElasticsearchManager 尚未启动或配置无效")
        return self._client

    def get_active_client(self) -> Optional[AsyncElasticsearch]:
        """获取 ES 客户端（安全模式）

        用于健康检查等场景，客户端不可用时返回 None 而非抛异常

        Returns:
            Optional[AsyncElasticsearch]: 客户端实例或 None
        """
        return self._client

    async def start(self) -> None:
        """启动资源管理器

        执行流程：订阅配置事件 -> 首次加载配置 -> 启动健康检查

        Raises:
            Exception: 首次加载失败时向上抛出
        """
        if self._started:
            return

        logger.info("正在启动 ElasticsearchManager...")

        # EventBus 依赖 _on_config_updated 中的守卫逻辑
        self._event_bus.subscribe(
            EventConstants.CONFIG_UPDATED,
            self._on_config_updated
        )

        try:
            await self.reload()
        except Exception:
            logger.critical("ElasticsearchManager 启动失败")
            raise

        await self._health_watcher.start()
        self._started = True

    async def stop(self) -> None:
        """停止资源管理器

        按顺序执行：停止 Watcher -> 取消后台任务 -> 关闭客户端
        """
        logger.info("正在停止 ElasticsearchManager...")

        await self._health_watcher.stop()

        # 清理延迟关闭旧连接的后台任务
        if self._background_tasks:
            for task in self._background_tasks:
                task.cancel()
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

        if self._client:
            await self._client.close()
            self._client = None

        # 标记停止后，后续配置变更事件将被忽略
        self._started = False

    async def _on_config_updated(self, _event: Event) -> None:
        """配置变更事件回调

        Args:
            _event: 配置更新事件（未使用具体内容）
        """
        # 守卫：资源已停止时忽略事件，因为 EventBus 不支持取消订阅
        if not self._started:
            return

        try:
            await self.reload()
        except Exception:
            # 仅记录日志，避免异常影响 EventBus worker
            logger.exception("ES 配置热重载失败")

    async def reload(self) -> None:
        """热重载配置并重建连接

        配置未变更时跳过重连；重连失败时保持旧配置（仅在已启动状态下）

        Raises:
            ValueError: 首次加载时配置无效
            ConnectionError: 首次加载时无法连接 ES
        """
        async with self._reload_lock:
            new_config = self._load_config()

            if not new_config:
                if not self._started:
                    raise ValueError("ES 配置无效或缺失")
                else:
                    logger.error("新配置无效，保持旧配置")
                    return

            # 配置未变更且客户端存在时跳过
            if (self._config and
                    self._config.model_dump() == new_config.model_dump() and
                    self._client):
                return

            try:
                logger.debug(f"正在重连 ES: {new_config.hosts}")
                new_client = await self._init_client(new_config)

                old_client = self._client
                self._client = new_client
                self._config = new_config

                # 延迟关闭旧连接，给进行中的请求留出完成时间
                if old_client:
                    task = asyncio.create_task(self._safe_close_client(old_client))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)

                logger.success("ElasticsearchManager 重载成功")

            except Exception:
                if not self._started:
                    raise
                logger.exception("ES 重连失败")

    def _build_auth_params(self, config: ElasticsearchConfig) -> Dict[str, Any]:
        """构建认证参数

        Args:
            config: ES 配置对象

        Returns:
            Dict[str, Any]: 包含 api_key 或 basic_auth 的字典，或空字典
        """
        if config.api_key:
            return {"api_key": config.api_key}
        if config.username and config.password:
            return {"basic_auth": (config.username, config.password)}
        return {}

    def _build_ssl_params(self, config: ElasticsearchConfig) -> Dict[str, Any]:
        """构建 SSL/TLS 参数

        Args:
            config: ES 配置对象

        Returns:
            Dict[str, Any]: SSL 相关参数字典
        """
        ssl_params: Dict[str, Any] = {"verify_certs": config.verify_certs}
        if config.ca_certs:
            ssl_params["ca_certs"] = config.ca_certs
        if config.client_cert and config.client_key:
            logger.info("启用 mTLS 双向认证")
            ssl_params["client_cert"] = config.client_cert
            ssl_params["client_key"] = config.client_key
        return ssl_params

    async def _init_client(
        self,
        config: ElasticsearchConfig
    ) -> AsyncElasticsearch:
        """初始化 ES 客户端并验证连接

        Args:
            config: ES 配置对象

        Returns:
            AsyncElasticsearch: 已验证连接的客户端实例

        Raises:
            ConnectionError: 无法连接到 ES 节点
        """
        es_lib_version = getattr(elasticsearch, "__version__", "unknown")
        logger.debug(f"初始化 ES 连接: {config.hosts} | Lib: v{es_lib_version}")

        client = AsyncElasticsearch(
            hosts=config.hosts,
            connections_per_node=config.max_connections,
            request_timeout=config.request_timeout,
            max_retries=config.max_retries,
            retry_on_timeout=config.retry_on_timeout,
            sniff_on_start=config.sniff_on_start,
            sniff_on_node_failure=config.sniff_on_connection_fail,
            sniff_timeout=config.sniffer_timeout,
            http_compress=True,
            **self._build_ssl_params(config),
            **self._build_auth_params(config)
        )

        try:
            info = await client.info()
            version = info.get('version', {}).get('number', 'unknown')
            logger.info(f"ES 连接成功! Server Version: {version}")
        except Exception as e:
            await client.close()
            raise ConnectionError(f"无法连接 ES 节点: {config.hosts} -> {e}")

        return client

    async def _safe_close_client(self, client: AsyncElasticsearch) -> None:
        """延迟关闭旧客户端

        等待一段时间后关闭，给进行中的请求留出完成窗口

        Args:
            client: 待关闭的客户端实例
        """
        try:
            # 5 秒宽限期，让进行中的请求有机会完成
            await asyncio.sleep(5)
            await client.close()
        except Exception as e:
            logger.warning(f"关闭旧 ES 客户端异常: {e}")

    def _load_config(self) -> Optional[ElasticsearchConfig]:
        """从 SettingsManager 加载并解析 ES 配置

        Returns:
            Optional[ElasticsearchConfig]: 解析成功返回配置对象，否则返回 None
        """
        try:
            ns = self._settings.local.apollo_namespace
            section = self._settings.get_section(ns)

            sys_conf_raw = section.values.get("system.config")
            if not sys_conf_raw:
                return None

            if isinstance(sys_conf_raw, str):
                sys_conf = json.loads(sys_conf_raw)
            else:
                sys_conf = sys_conf_raw

            es_conf = sys_conf.get("elasticsearch")
            if not es_conf:
                return None

            conf_data = es_conf.copy()
            conf_data.pop("enabled", None)

            # 强制使用 CWD/config 作为相对路径基准，确保路径可控
            config_dir = os.path.abspath("config")

            for field in ["ca_certs", "client_cert", "client_key"]:
                val = conf_data.get(field)
                if val and not Path(val).is_absolute():
                    full_path = os.path.join(config_dir, val)
                    conf_data[field] = full_path

            return ElasticsearchConfig(**conf_data)

        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"ES 配置解析失败: {e}")
            return None
        except ValidationError as e:
            logger.error(f"ES 配置校验失败: {e}")
            return None
        except Exception as e:
            logger.exception(f"加载 ES 配置未知错误: {e}")
            return None

    async def health_check(self) -> ResourceHealth:
        """执行健康检查并返回状态

        Returns:
            ResourceHealth: 包含集群状态、节点数等详情的健康报告
        """
        client = self._client
        if not client:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.INIT
            )

        try:
            health = await client.cluster.health()
            status_str = health.get("status", "red")
            # green/yellow 视为健康，red 视为不健康
            is_healthy = status_str in ("green", "yellow")

            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.HEALTHY if is_healthy else HealthStatus.UNHEALTHY,
                details={
                    "cluster_name": health.get("cluster_name"),
                    "status": status_str,
                    "number_of_nodes": health.get("number_of_nodes")
                }
            )
        except Exception as e:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                last_error=str(e)
            )
