from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Mapping, Protocol, runtime_checkable, Tuple, override

import httpx
import yaml
from loguru import logger

from .base import HealthStatus, Resource, ResourceHealth
from .events import Event, EventBus, EventConstants
from .limits import ResourceLimiter
from .watcher import BackgroundWatcher, DEFAULT_TICK_TIMEOUT, STOP_TIMEOUT
from ..config.constants import ResourceKind, ENV_CACHE_ENCRYPTION_KEY
from ..config.runtime_store import RuntimeConfigStore
from ..config.settings_manager import SettingsManager


@runtime_checkable
class ApolloClientLike(Protocol):
    """Apollo 客户端协议接口

    定义了 Apollo 配置客户端必须实现的核心方法，用于运行时的类型检查和依赖注入
    通过 Protocol 实现鸭子类型，允许任何符合此接口的对象作为客户端使用（便于测试 Mock 或替换实现）

    Methods:
        fetch_all: 从 Apollo 服务器获取所有命名空间的配置
        save_cache: 将配置持久化到本地缓存
        load_cache: 从本地缓存加载配置
    """

    async def fetch_all(self) -> Mapping[str, Mapping[str, Any]]:
        """从 Apollo 服务器获取所有命名空间的配置

        Returns:
            Mapping[str, Mapping[str, Any]]: 命名空间名称到配置字典的映射
        """
        ...

    async def save_cache(self, sections: Mapping[str, Mapping[str, Any]]) -> None:
        """将配置持久化到本地缓存文件

        Args:
            sections (Mapping[str, Mapping[str, Any]]): 要缓存的配置数据，按命名空间组织
        """
        ...

    async def load_cache(self) -> Mapping[str, Mapping[str, Any]]:
        """从本地缓存文件加载配置

        Returns:
            Mapping[str, Mapping[str, Any]]: 缓存中的配置数据，按命名空间组织。若加载失败则返回空字典
        """
        ...


class AsyncApolloClient:
    """基于 httpx 的企业级异步 Apollo 配置中心客户端

    该客户端实现了完整的 Apollo 配置拉取、缓存管理和安全机制，具备生产级特性：

    核心特性：
    1. 并发拉取：使用 `asyncio.gather` 并发拉取多个 Namespace，并通过信号量 (`Semaphore`) 限制并发数，防止击穿服务器。
    2. 增量更新：支持 `ReleaseKey` 机制，只在配置变更时解析内容，减少网络和 CPU 开销。
    3. 安全存储：
       - 本地缓存使用 XOR + Base64 进行混淆/加密，防止敏感配置明文落地。
       - 支持通过环境变量注入密钥，或自动降级使用 AppID 生成的兜底密钥。
    4. 原子写入：使用临时文件 + `os.replace` 确保缓存写入的原子性，防止进程崩溃导致文件损坏。
    5. 安全防御：内置 SSRF 防御（检查 URL 协议）和文件权限控制（600）。

    Attributes:
        self._server_url (str): Apollo 服务器地址
        self._app_id (str): 应用 ID
        self._cluster (str): 集群名称
        self._namespaces (list[str]): 目标命名空间列表
        self._secret (str | None): HMAC 签名密钥（可选）
        self._ip (str | None): 客户端 IP（用于灰度路由）
        self._timeout (float): 请求超时时间
        self._cache_dir (Path | None): 本地缓存目录
        self._obfuscation_key (bytes): 用于缓存加密的密钥
        self._release_keys (Dict[str, str]): 各命名空间的版本控制 Key
        self._semaphore (asyncio.Semaphore): 并发控制信号量
    """

    def __init__(
            self,
            server_url: str,
            app_id: str,
            cluster: str = "default",
            namespaces: list[str] | None = None,
            secret: str | None = None,
            ip: str | None = None,
            timeout: float = 10.0,
            cache_dir: Path | None = None,
            encryption_key: str | None = None,
    ) -> None:
        """初始化 Apollo 客户端

        Args:
            server_url (str): Apollo 配置服务器 URL
            app_id (str): 应用 ID
            cluster (str): 集群名称
            namespaces (list[str] | None): 命名空间列表
            secret (str | None): 访问密钥（用于 HMAC 签名）
            ip (str | None): 客户端 IP
            timeout (float): HTTP 超时时间
            cache_dir (Path | None): 本地缓存目录。若为 None，则禁用文件缓存功能。
            encryption_key (str | None): 用于缓存加密的密钥。若为 None，将使用默认策略生成。

        Raises:
            ValueError: 当 server_url 协议不安全（非 HTTP/HTTPS）时抛出
        """
        self._server_url = server_url.rstrip("/")

        # SSRF 基础防御：严格限制协议为 HTTP 或 HTTPS
        # 防止攻击者传入 file://etc/passwd 等恶意 URL 读取本地文件
        if not self._server_url.startswith(("http://", "https://")):
            raise ValueError(f"Apollo URL 协议无效: {self._server_url}")

        self._app_id = app_id
        self._cluster = cluster
        self._namespaces = namespaces or ["application"]
        self._secret = secret
        self._ip = ip
        self._timeout = timeout
        self._cache_dir = cache_dir

        # 初始化缓存混淆密钥
        # 策略：优先使用传入的密钥（通常来自环境变量），否则使用 AppID 生成兜底密钥
        if encryption_key:
            self._obfuscation_key = encryption_key.encode("utf-8")
        else:
            # 兜底策略：使用 AppID 的 SHA256 哈希作为密钥
            # 只有在启用了缓存（cache_dir 不为空）时才打印警告，避免在占位符实例中产生噪音
            if cache_dir:
                logger.warning(
                    f"⚠️ 未检测到环境变量 {ENV_CACHE_ENCRYPTION_KEY}！"
                    "正在使用默认密钥对 Apollo 缓存进行混淆。"
                    f"建议在生产环境中配置 {ENV_CACHE_ENCRYPTION_KEY} 以增强安全性。"
                )
            self._obfuscation_key = hashlib.sha256(app_id.encode("utf-8")).digest()

        # 用于记录每个命名空间的最新版本 Key，实现 HTTP 304 增量更新逻辑
        self._release_keys: Dict[str, str] = {}
        # 限制同时发起的 HTTP 请求数量，保护 Apollo Server
        self._semaphore = asyncio.Semaphore(5)

    def _obfuscate(self, content: str) -> bytes:
        """对配置内容进行混淆（Base64 + XOR）

        这是一个轻量级的加密实现，主要防止明文泄露

        Args:
            content (str): 明文配置内容 (JSON 字符串)

        Returns:
            bytes: 加密后的字节流
        """
        data_bytes = content.encode("utf-8")
        key_len = len(self._obfuscation_key)
        # 循环 XOR 加密：plaintext ^ key = ciphertext
        xor_bytes = bytearray(b ^ self._obfuscation_key[i % key_len] for i, b in enumerate(data_bytes))
        return base64.b64encode(xor_bytes)

    def _deobfuscate(self, content: bytes) -> str:
        """对缓存内容进行反混淆

        Args:
            content (bytes): 加密后的字节流

        Returns:
            str: 解密后的明文配置内容

        Raises:
            Exception: 如果 Base64 解码失败或编码错误
        """
        # Base64 解码
        xor_bytes = base64.b64decode(content)
        key_len = len(self._obfuscation_key)
        # XOR 还原：ciphertext ^ key = plaintext
        data_bytes = bytearray(b ^ self._obfuscation_key[i % key_len] for i, b in enumerate(xor_bytes))
        return data_bytes.decode("utf-8")

    async def fetch_all(self) -> Mapping[str, Mapping[str, Any]]:
        """并发拉取所有命名空间的配置

        使用 asyncio.gather 并发执行，提高启动速度。
        单个命名空间失败不会中断整体流程，确保系统的健壮性。

        Returns:
            Mapping[str, Mapping[str, Any]]: 成功获取的配置字典（Key=Namespace）
        """
        results: Dict[str, Mapping[str, Any]] = {}

        # 使用共享的 Client Session，复用 TCP 连接（Keep-Alive）
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            # 创建并发任务列表
            tasks = [self._fetch_one_safe(client, ns) for ns in self._namespaces]
            # 执行任务，return_exceptions=True 确保个别失败不影响整体
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for ns, resp in zip(self._namespaces, responses):
                if isinstance(resp, Exception):
                    logger.warning(f"获取命名空间 '{ns}' 失败: {resp}")
                    continue
                # 如果响应不为空（非 304），则添加到结果中
                if resp:
                    results[ns] = resp
        return results

    async def _fetch_one_safe(self, client: httpx.AsyncClient, namespace: str) -> Dict[str, Any]:
        """带信号量控制的单个命名空间拉取封装"""
        # 进入临界区，限制并发数
        async with self._semaphore:
            return await self._fetch_one(client, namespace)

    async def _fetch_one(self, client: httpx.AsyncClient, namespace: str) -> Dict[str, Any]:
        """从 Apollo 拉取单个命名空间的配置（核心逻辑）

        流程：
        1. 构造带 IP 和 ReleaseKey 的 URL
        2. 生成 HMAC 签名头
        3. 发起请求并处理 304/200 状态码
        4. 解析不同格式的配置内容 (Properties/YAML/JSON)
        """
        url = f"{self._server_url}/configs/{self._app_id}/{self._cluster}/{namespace}"
        params = {}

        if self._ip:
            params["ip"] = self._ip

        # 携带本地版本号，支持服务端 304 响应
        last_release_key = self._release_keys.get(namespace, "")
        if last_release_key:
            params["releaseKey"] = last_release_key

        headers = self._sign_headers(url)

        try:
            resp = await client.get(url, params=params, headers=headers)

            # 304 Not Modified: 配置未变更，无需处理
            if resp.status_code == 304:
                return {}

            if resp.status_code != 200:
                logger.warning(f"Apollo 获取失败 [{resp.status_code}]: {url}")
                return {}

            data = resp.json()
            current_release_key = data.get("releaseKey")

            # 双重检查：ReleaseKey 未变也视为无更新
            if current_release_key and current_release_key == last_release_key:
                return {}

            configurations = self._parse_configurations(data, namespace)

            # 更新本地版本号
            if current_release_key:
                self._release_keys[namespace] = current_release_key

            return configurations

        except httpx.RequestError as exc:
            logger.warning(f"获取 {namespace} 时发生网络错误: {exc}")
            raise
        except Exception as exc:
            logger.error(f"解析 {namespace} 的响应时发生未预期错误: {exc}")
            raise

    async def save_cache(self, sections: Mapping[str, Mapping[str, Any]]) -> None:
        """原子化保存配置到本地缓存文件（支持加密）

        使用原子写入模式（Atomic Write）：先写临时文件，再重命名。
        确保在写入过程中即便断电或 Crash，也不会留下损坏的文件。
        """
        if not self._cache_dir:
            return

        target_file = self._cache_dir / f"apollo_cache_{self._app_id}_{self._cluster}.dat"
        # 包装时间戳，便于后续扩展过期检查
        payload = {"timestamp": time.time(), "sections": sections}

        try:
            # 1. 序列化 + 加密
            json_str = json.dumps(payload, ensure_ascii=False, indent=None)
            encrypted_content = self._obfuscate(json_str)
        except Exception as e:
            logger.error(f"序列化或加密缓存失败: {e}")
            return

        loop = asyncio.get_running_loop()

        def _atomic_write() -> None:
            """同步 IO 操作，将在线程池中执行"""
            # 确保目录存在并设置权限为 700 (rwx------)
            if self._cache_dir and not self._cache_dir.exists():
                self._cache_dir.mkdir(parents=True, exist_ok=True)
                try:
                    os.chmod(self._cache_dir, 0o700)
                except Exception:
                    pass

            # 创建临时文件
            with tempfile.NamedTemporaryFile(mode='wb', dir=self._cache_dir, delete=False) as tmp:
                tmp.write(encrypted_content)
                tmp.flush()
                # 强制刷盘，确保数据写入物理介质
                os.fsync(tmp.fileno())
                tmp_name = tmp.name

            # 设置文件权限为 600
            try:
                os.chmod(tmp_name, 0o600)
            except Exception:
                pass

            # 原子替换：这是 POSIX 标准保证的操作
            try:
                os.replace(tmp_name, target_file)
            except OSError:
                os.remove(tmp_name)
                raise

        try:
            await loop.run_in_executor(None, _atomic_write)
            logger.debug(f"Apollo 配置已加密并缓存到 {target_file}")
        except Exception as exc:
            logger.error(f"写入 Apollo 缓存失败: {exc}")

    def _load_encrypted_cache(self, dat_file: Path) -> Dict[str, Any] | None:
        """[Helper] 尝试加载并解密 .dat 缓存文件

        Returns:
            Dict | None: 解析成功返回字典，失败或不存在返回 None
        """
        if not dat_file.exists():
            return None
        try:
            content = dat_file.read_bytes()
            json_str = self._deobfuscate(content)
            return json.loads(json_str)
        except Exception as e:
            logger.warning(f"读取加密缓存失败 ({dat_file}): {e}")
            return None

    def _load_legacy_cache(self, json_file: Path) -> Dict[str, Any] | None:
        """[Helper] 尝试加载旧版明文 .json 缓存文件（兼容性支持）"""
        if not json_file.exists():
            return None

        logger.info(f"检测到旧版明文缓存，正在尝试加载: {json_file}")
        try:
            content_str = json_file.read_text(encoding="utf-8")
            return json.loads(content_str)
        except Exception as e:
            logger.warning(f"读取明文缓存失败 ({json_file}): {e}")
            return None

    async def load_cache(self) -> Mapping[str, Mapping[str, Any]]:
        """从本地缓存文件加载配置（支持自动解密与降级）

        策略：
        1. 优先读取 .dat 加密文件。
        2. 如果失败或不存在，尝试读取 .json 明文文件（用于从旧版本平滑迁移）。
        """
        if not self._cache_dir:
            return {}

        dat_file = self._cache_dir / f"apollo_cache_{self._app_id}_{self._cluster}.dat"
        json_file = self._cache_dir / f"apollo_cache_{self._app_id}_{self._cluster}.json"

        loop = asyncio.get_running_loop()

        def _read_task() -> Dict[str, Any]:
            # 1. 尝试加密文件
            data = self._load_encrypted_cache(dat_file)
            if data is not None:
                return data

            # 2. 尝试旧版文件
            data = self._load_legacy_cache(json_file)
            if data is not None:
                return data

            return {}

        try:
            payload = await loop.run_in_executor(None, _read_task)

            # 校验数据结构完整性
            if not isinstance(payload, dict) or "sections" not in payload:
                if payload:
                    logger.warning("缓存文件结构无效（缺少 sections 字段）。")
                return {}

            sections = payload.get("sections", {})
            logger.info(f"从本地缓存恢复了 {len(sections)} 个配置节")
            return sections
        except Exception as exc:
            logger.warning(f"加载 Apollo 缓存异常: {exc}")
            return {}

    def _parse_configurations(self, data: Dict[str, Any], namespace: str) -> Dict[str, Any]:
        """解析 Apollo 返回的配置内容，支持多种格式 (JSON, YAML, Properties)"""
        configurations = data.get("configurations", {})

        # 如果不是特殊格式后缀，默认按 properties 处理（直接返回 KV）
        if not (namespace.endswith(".yaml") or namespace.endswith(".yml") or namespace.endswith(".json")):
            return configurations

        content = configurations.get("content")
        if content is None:
            return configurations

        try:
            if namespace.endswith(".yaml") or namespace.endswith(".yml"):
                parsed = yaml.safe_load(content)
                return parsed if isinstance(parsed, dict) else {}

            if namespace.endswith(".json"):
                parsed = json.loads(content)
                return parsed if isinstance(parsed, dict) else {}
        except Exception as e:
            logger.error(f"解析配置内容失败 [{namespace}]: {e}")
            return {}

        return configurations

    def _sign_headers(self, url: str) -> Dict[str, str]:
        """生成 Apollo HMAC-SHA1 签名认证头"""
        if not self._secret:
            return {}

        headers = {}
        try:
            path_start = url.index("/", 8)
            path_with_query = url[path_start:]
        except ValueError:
            path_with_query = "/"

        timestamp = str(int(round(time.time() * 1000)))
        string_to_sign = f"{timestamp}\n{path_with_query}"
        hmac_code = hmac.new(self._secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1).digest()
        signature = base64.b64encode(hmac_code).decode("utf-8")

        headers["Authorization"] = f"Apollo {self._app_id}:{signature}"
        headers["Timestamp"] = timestamp

        return headers


class ApolloConfigWatcher(BackgroundWatcher):
    """Apollo 配置后台轮询器

    继承自 BackgroundWatcher，定期从 Apollo 拉取配置并更新到 RuntimeStore。
    """

    def __init__(
            self,
            client: ApolloClientLike,
            runtime_store: RuntimeConfigStore,
            settings: SettingsManager,
            limiter: ResourceLimiter | None = None,
            event_bus: EventBus | None = None,
            **kwargs: Any,
    ) -> None:
        super().__init__(
            limiter=limiter,
            event_bus=event_bus,
            **kwargs
        )
        self._client = client
        self._runtime_store = runtime_store
        self._settings = settings
        self._limiter = limiter
        self._event_bus = event_bus

        self.last_error: str | None = None
        self.last_success_ts: float | None = None

    @override
    async def tick(self) -> None:
        """执行单次配置同步任务"""
        try:
            await self._sync_logic()
            self.last_error = None
            self.last_success_ts = time.time()
        except Exception as exc:
            self.last_error = repr(exc)
            # 异常向上抛出，由基类处理日志和退避
            raise

    async def _sync_logic(self) -> None:
        """配置同步核心逻辑：拉取 -> 更新内存 -> 持久化缓存"""
        raw_sections = await self._client.fetch_all()

        if not raw_sections:
            return

        # 确保类型安全
        sections: Dict[str, Dict[str, Any]] = {}
        for section_name, section in raw_sections.items():
            if isinstance(section, Mapping):
                sections[str(section_name)] = dict(section)

        if not sections:
            return

        # 1. 更新运行时内存配置
        await self._runtime_store.update_sections(sections, source="apollo")
        # 2. 持久化到本地缓存（加密）
        await self._client.save_cache(sections)
        # 3. 动态调整轮询间隔
        self.refresh_timeouts()

        # 4. 发布配置更新事件
        if self._event_bus:
            updated_namespaces = list(sections.keys())
            await self._event_bus.publish(
                Event(
                    type=EventConstants.CONFIG_UPDATED,
                    payload={
                        EventConstants.KEY_SOURCE: "apollo",
                        EventConstants.KEY_UPDATED_NAMESPACES: updated_namespaces,
                        EventConstants.KEY_TIMESTAMP: time.time()
                    }
                )
            )

        logger.debug(f"Apollo 同步成功: 更新了 {len(sections)} 个配置节")

    def refresh_timeouts(self) -> None:
        """从新配置中读取并动态更新轮询间隔"""
        try:
            tick_to, stop_to = self._load_timeouts_from_settings()

            if tick_to > 0 and tick_to != self._tick_timeout:
                logger.info(f"监视器 tick_timeout 已更新: {self._tick_timeout} -> {tick_to}")
                self._tick_timeout = tick_to

            if stop_to > 0 and stop_to != self._stop_timeout:
                logger.info(f"Watcher stop_timeout updated: {self._stop_timeout} -> {stop_to}")
                self._stop_timeout = stop_to
        except Exception as e:
            logger.warning(f"刷新监视器超时失败: {e}")

    def _load_timeouts_from_settings(self) -> Tuple[float, float]:
        default_ns = self._settings.local.apollo_namespace
        section = self._settings.get_section(default_ns)

        tick = DEFAULT_TICK_TIMEOUT
        stop = STOP_TIMEOUT

        if section:
            sys_config_raw = section.values.get("system.config")
            if sys_config_raw:
                try:
                    sys_config = sys_config_raw
                    if isinstance(sys_config, str):
                        sys_config = json.loads(sys_config)

                    if isinstance(sys_config, dict):
                        timeouts = sys_config.get("timeouts", {})
                        tick = float(timeouts.get("watcher_tick_timeout", tick))
                        stop = float(timeouts.get("watcher_stop_timeout", stop))
                except Exception as e:
                    logger.warning(f"解析 system.config 超时配置错误: {e}")

        return tick, stop


class ApolloResource(Resource):
    """Apollo 资源管理器

    该类实现了 Apollo 配置中心的完整生命周期管理。
    作为系统启动的第一个资源（Startup Priority 0），它负责初始化配置环境。
    """

    def __init__(
            self,
            settings: SettingsManager,
            *,
            name: str = "apollo",
            namespaces: list[str] | None = None,
            poll_interval: float = 10.0,
            limiter: ResourceLimiter | None = None,
            event_bus: EventBus | None = None,
            startup_priority: int = 0,
    ) -> None:
        """初始化 Apollo 资源管理器"""
        super().__init__(
            name=name, kind=ResourceKind.INFRA, limiter=limiter, startup_priority=startup_priority
        )
        self._settings = settings
        self._runtime_store = settings.runtime_store
        self._event_bus = event_bus
        self._started = False

        local = settings.local

        # [Note] 显式保存命名空间列表，供 start() 使用
        self._namespaces = namespaces or [local.apollo_namespace]

        # [Important] 创建一个“占位符”客户端实例
        # 此时设置 cache_dir=None 和 encryption_key=None，
        # 目的是避免在 __init__ 阶段触发关于“缺失密钥”的 Warning 日志。
        # 真正的、功能完整的 Client 将在 start() 阶段创建。
        self._client = AsyncApolloClient(
            server_url=str(local.apollo_server_url),
            app_id=local.apollo_app_id,
            cluster=local.apollo_cluster,
            namespaces=self._namespaces,
            secret=None,
            timeout=10.0,
            cache_dir=None,
            encryption_key=None
        )

        # 创建后台轮询器实例
        self._watcher = ApolloConfigWatcher(
            name=f"{name}-watcher",
            logger=logger,
            client=self._client,
            runtime_store=self._runtime_store,
            settings=settings,
            limiter=limiter,
            event_bus=event_bus,
            min_interval=poll_interval,
            max_interval=poll_interval * 6,
            jitter=0.1,
            tick_timeout=DEFAULT_TICK_TIMEOUT,
            stop_timeout=STOP_TIMEOUT
        )

    @override
    async def start(self) -> None:
        """启动 Apollo 资源

        启动流程：
        1. 读取环境变量配置（如加密密钥）。
        2. 重建 Client 实例（注入真实的 cache_dir 和 encryption_key）。
        3. 尝试同步拉取配置（优先网络，失败则降级到缓存）。
        4. 启动后台轮询器。
        """
        if self._started:
            return

        logger.info(f"正在启动 ApolloResource[{self.name}] (Priority: {self.startup_priority})...")

        # 延迟读取环境变量，仅在 start 阶段读取，便于在测试环境或容器启动脚本中注入变量
        cache_key = os.getenv(ENV_CACHE_ENCRYPTION_KEY)

        local = self._settings.local
        cache_dir = self._settings.paths.data_dir / "apollo_cache"

        # 创建真正的全功能客户端，注入真实的缓存目录和密钥，此时如果缺少密钥且启用了缓存，才会打印 Warning
        self._client = AsyncApolloClient(
            server_url=str(local.apollo_server_url),
            app_id=local.apollo_app_id,
            cluster=local.apollo_cluster,
            namespaces=self._namespaces,
            secret=None,
            timeout=10.0,
            cache_dir=cache_dir,
            encryption_key=cache_key
        )
        # 更新 Watcher 持有的引用
        self._watcher._client = self._client

        init_success = False

        try:
            logger.info("正在尝试从网络同步初始配置...")
            # 手动执行一次 Tick，作为启动时的同步阻塞检查
            await self._watcher.tick()
            init_success = True
        except Exception as net_exc:
            logger.warning(f"Apollo 初始网络同步失败: {net_exc}")
            logger.info("正在尝试从本地缓存加载配置...")

            try:
                # 网络失败，尝试降级到本地缓存
                cached_sections = await self._client.load_cache()

                if cached_sections:
                    await self._runtime_store.update_sections(cached_sections, source="apollo_cache")
                    self._watcher.refresh_timeouts()
                    init_success = True
                    logger.warning("Apollo 从本地缓存初始化 (运行在降级模式下).")
                else:
                    logger.error("本地缓存为空或缺失。")
            except Exception as cache_exc:
                logger.error(f"加载本地缓存失败: {cache_exc}")

            if not init_success:
                # 只有当网络和缓存都不可用时，才抛出致命错误，阻止应用启动
                raise RuntimeError("初始化 Apollo 配置失败 (网络失败且缓存缺失)") from net_exc

        # 启动后台轮询
        await self._watcher.start()
        self._started = True

    @override
    async def stop(self) -> None:
        """停止 Apollo 资源（优雅停止后台轮询器）"""
        if not self._started:
            return

        await self._watcher.stop()
        self._started = False

    @override
    async def health_check(self) -> ResourceHealth:
        """执行健康检查"""
        if not self._started:
            return ResourceHealth(kind=self.kind, name=self.name, status=HealthStatus.INIT, message="Not started")

        if self._watcher.last_error:
            return ResourceHealth(
                kind=self.kind, name=self.name, status=HealthStatus.DEGRADED,
                message="Background sync failed", last_error=self._watcher.last_error,
                details={"last_success_ts": self._watcher.last_success_ts}
            )

        return ResourceHealth(
            kind=self.kind, name=self.name, status=HealthStatus.HEALTHY,
            message="In sync", details={"last_success_ts": self._watcher.last_success_ts}
        )

    def __repr__(self) -> str:
        return f"<ApolloResource 名称={self.name} 已启动={self._started}>"
