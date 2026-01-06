from __future__ import annotations

import asyncio
import json
from typing import Dict, Optional, List, Any

import httpx
import tiktoken
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage
from loguru import logger

from .base.config import LLMConfig, OpenAIModelConfig
from .drivers.china_unicom import ChinaUnicomChatModel
from .drivers.openai import build_openai_model
from ..config.constants import ResourceName, ResourceKind
from ...core.config import SettingsManager
from ...core.resources.base import Resource, ResourceHealth, HealthStatus
from ...core.resources.events import Event, EventBus, EventConstants
from ...core.resources.limits import ResourceLimiter


class LLMRuntime:
    """LLM 运行时封装（模型实例 + 流控 + 连接池）

    将单个 LLM 模型所需的全部资源封装为统一的运行时对象，实现资源隔离和生命周期管理

    增强特性：
    - 集成 EventBus：将 EventBus 注入 ResourceLimiter，激活限流排队告警能力

    核心设计特点：
        - 资源绑定：将 Config、Model、Limiter、HTTPClient 绑定为一个整体
        - 流控隔离：每个模型独立的并发控制和 QPS 限制
        - 连接池复用：使用 httpx.AsyncClient 实现 HTTP/2 连接池复用
        - 多供应商支持：通过工厂模式支持 OpenAI、中国联通等不同供应商

    Attributes:
        alias (str): 模型别名（用于日志和监控标识）
        config (OpenAIModelConfig): 模型配置对象（包含 URL、密钥、超时等）
        is_active (bool): 运行时状态标记（False 时禁止新请求，用于优雅下线）
        limiter (Optional[ResourceLimiter]): 流控器实例（如果配置了 max_concurrency 或 qps）
        client (httpx.AsyncClient): HTTP 异步客户端（复用连接池，支持 HTTP/2）
        model (BaseChatModel): LangChain 标准模型实例（实际执行推理的对象）
    """

    def __init__(
            self,
            alias: str,
            config: OpenAIModelConfig,
            connection_limits: Dict[str, int],
            event_bus: Optional[EventBus] = None
    ) -> None:
        """初始化 LLM 运行时实例

        Args:
            alias (str): 模型别名，用于日志标识和监控
            config (OpenAIModelConfig): 模型配置对象
            connection_limits (Dict[str, int]): HTTP 连接池配置
            event_bus (Optional[EventBus]): 事件总线，用于限流器告警
        """
        self.alias = alias
        # 模型配置对象
        self.config = config
        self.is_active = True

        # 流控器用于限制并发请求数和 QPS，防止后端过载
        self.limiter: Optional[ResourceLimiter] = None
        if config.max_concurrency or config.qps:
            # 注入 event_bus 和 name，激活限流器的可观测性能力
            self.limiter = ResourceLimiter(
                max_concurrency=config.max_concurrency,
                qps=config.qps,
                name=f"llm-limiter-{alias}",
                event_bus=event_bus
            )

        # 从连接池配置中提取参数
        keepalive = connection_limits.get("max_keepalive_connections", 20)
        max_conns = connection_limits.get("max_connections", 100)

        # 创建 httpx.AsyncClient 实例（复用连接池，支持 HTTP/2）
        self.client = httpx.AsyncClient(
            timeout=config.timeout,
            limits=httpx.Limits(
                max_keepalive_connections=keepalive,
                max_connections=max_conns
            )
        )

        # 构建模型实例
        self.model: BaseChatModel = self._build_model()

    def _build_model(self) -> BaseChatModel:
        """根据 provider_type 构建不同供应商的模型实例"""
        # 1. 根据供应商类型分发到不同的构建方法
        if self.config.provider_type == "china_unicom":
            # 中国联通专用构建逻辑（处理特殊认证协议）
            return self._build_unicom_model()
        elif self.config.provider_type == "openai":
            # OpenAI 官方 API 构建逻辑（使用标准 OpenAI SDK）
            return build_openai_model(self.config)
        else:
            # 2. 未知供应商类型，抛出异常
            raise ValueError(f"未知的大模型提供商: {self.config.provider_type}")

    def _build_unicom_model(self) -> ChinaUnicomChatModel:
        """构建中国联通专用模型实例（处理特殊认证协议）"""
        # 1. 提取 scene_code（场景码），按优先级尝试
        scene_code = (self.config.scene_code or self.config.default_headers.get("scene_code"))

        # 2. 构造联通模型实例，映射配置参数
        return ChinaUnicomChatModel(
            client=self.client,
            model=self.config.model_name,
            api_url=self.config.base_url or "",
            app_id=self.config.app_id,
            app_secret=self.config.app_secret,
            scene_code=scene_code,
            timeout=self.config.timeout,
            default_headers=self.config.default_headers,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
            top_p=self.config.top_p,
            api_key=self.config.api_key,
            model_kwargs=self.config.model_kwargs
        )

    def estimate_tokens(self, messages: List[BaseMessage]) -> int:
        """估算消息列表的 Token 数量（用于上下文长度保护）"""
        # 1. 提取所有文本内容（跳过非字符串消息，如图片消息）
        text = "".join([
            m.content for m in messages
            if isinstance(m.content, str)
        ])

        try:
            # 2. 尝试使用 tiktoken 精确计算（OpenAI 官方编码器）
            enc = tiktoken.get_encoding("cl100k_base")
            # 编码文本并返回 Token 数量
            return len(enc.encode(text))
        except Exception:
            # 3. tiktoken 不可用（未安装或编码失败），使用启发式规则
            # 字符数 ÷ 2（粗略估算，1 个中文字符 ≈ 2 个 Token）
            return len(text) // 2

    async def teardown(self) -> None:
        """异步清理资源（优雅关闭运行时）"""
        # 1. 标记运行时为非活跃状态（拒绝新请求）
        self.is_active = False

        # 2. 关闭 HTTP 异步客户端（释放连接池）
        if self.client:
            # aclose() 会优雅关闭所有连接（等待正在传输的数据完成）
            await self.client.aclose()

        # 3. 让出事件循环控制权（确保其他协程有机会执行）
        await asyncio.sleep(0)


class LLMManager(Resource):
    """LLM 资源管理器（企业级模型生命周期管理）

    统一管理所有 LLM 运行时实例，提供配置热加载、健康检查、故障隔离等企业级特性

    Attributes:
        _settings (SettingsManager): 配置管理器（访问 Apollo 配置）
        _event_bus (EventBus): 事件总线（订阅配置更新事件）
        _runtimes (Dict[str, LLMRuntime]): 活跃的运行时实例字典（key=模型别名）
        _reload_lock (asyncio.Lock): 重载锁（防止并发重载导致的竞态条件）
        _started (bool): 启动状态标记（防止重复启动）
        _default_alias (Optional[str]): 默认模型别名（用于兜底路由）
        _last_config_signature (Any): 上次配置的指纹（用于去重）
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus) -> None:
        """初始化 LLM 资源管理器"""
        # 1. 调用父类构造函数（注册为系统资源）
        # 显式指定 startup_priority=20（业务层），确保在数据库（priority=10）之后启动
        super().__init__(
            name=ResourceName.LLM,
            kind=ResourceKind.LLM,
            startup_priority=20
        )

        # 2. 保存依赖注入的对象
        self._settings = settings
        self._event_bus = event_bus

        # 3. 初始化内部状态
        self._runtimes: Dict[str, LLMRuntime] = {}
        self._reload_lock = asyncio.Lock()
        self._started = False
        self._default_alias: Optional[str] = None

        # 4. 配置指纹缓存
        self._last_config_signature: Any = None

    async def start(self) -> None:
        """启动 LLM 资源管理器（订阅事件，加载配置）"""
        # 1. 检查启动状态（防止重复启动）
        if self._started:
            return  # 已启动，直接返回（幂等性保证）

        # 2. 记录启动日志
        logger.info("正在启动 LLMManager...")

        # 3. 订阅配置更新事件
        self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        # 4. 加载初始配置（创建运行时实例）
        await self.reload()

        # 5. 标记为已启动状态
        self._started = True

        # 6. 记录启动完成日志
        if self._runtimes:
            logger.info(f"LLMManager 已启动。活跃模型: {list(self._runtimes.keys())}")
        else:
            logger.warning("LLMManager 已启动，但未加载任何活跃模型。")

    async def stop(self) -> None:
        """停止 LLM 资源管理器（关闭所有运行时，释放资源）"""
        # 1. 检查启动状态
        if not self._started:
            return  # 未启动，直接返回

        # 2. 记录停止日志
        logger.info("正在停止 LLMManager...")

        # 3. 加锁
        async with self._reload_lock:
            # 4. 创建销毁任务列表
            tasks = [r.teardown() for r in self._runtimes.values()]

            # 5. 并发执行所有销毁任务
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            # 6. 清空运行时字典
            self._runtimes.clear()

        # 7. 标记为未启动状态
        self._started = False

    async def health_check(self) -> ResourceHealth:
        """健康检查（返回管理器的运行状态）"""
        # 1. 检查启动状态
        if not self._started:
            return ResourceHealth(
                kind=self.kind,
                name=self.name,
                status=HealthStatus.INIT
            )

        # 2. 默认健康状态为 HEALTHY
        status = HealthStatus.HEALTHY
        msg = f"管理 {len(self._runtimes)} 个模型"

        # 3. 检查运行时数量
        if not self._runtimes:
            status = HealthStatus.DEGRADED
            msg = "未加载任何活跃模型"

        # 4. 返回健康状态对象
        return ResourceHealth(
            kind=self.kind,
            name=self.name,
            status=status,
            message=msg,
            details={"models": list(self._runtimes.keys())}
        )

    def get_runtime(self, alias: str) -> Optional[LLMRuntime]:
        """根据别名获取运行时实例（用于模型路由）"""
        return self._runtimes.get(alias)

    def get_default_runtime(self) -> Optional[LLMRuntime]:
        """获取默认运行时实例（用于兜底路由）"""
        # 1. 如果只有 1 个模型，直接返回
        if len(self._runtimes) == 1:
            return list(self._runtimes.values())[0]

        # 2. 如果配置了 default_alias，返回对应的运行时
        if self._default_alias and self._default_alias in self._runtimes:
            return self._runtimes.get(self._default_alias)

        # 3. 否则返回 None
        return None

    async def _on_config_updated(self, _event: Event) -> None:
        """配置更新事件回调（触发热加载）"""
        logger.debug("检测到配置更新，正在检查 LLMManager 是否需要重载...")
        try:
            await self.reload()
        except Exception as e:
            logger.error(f"热加载 LLMManager 失败: {e}")

    def _load_llm_config(self) -> Optional[LLMConfig]:
        """从配置中心加载模型配置（llm.models 键）"""
        # 1. 获取默认命名空间
        default_ns = self._settings.local.apollo_namespace
        # 2. 获取该命名空间的配置段
        section = self._settings.get_section(default_ns)

        if not section:
            logger.warning(f"默认命名空间 '{default_ns}' 为空或未加载。")
            return None

        # 3. 提取 llm.models 键的值
        raw_val = section.values.get("llm.models")

        if raw_val:
            try:
                data = raw_val
                if isinstance(raw_val, str):
                    data = json.loads(raw_val)

                if not isinstance(data, dict):
                    logger.error("'llm.models' 的配置值不是字典类型。")
                    return None

                return LLMConfig(models=data)
            except Exception as e:
                logger.error(f"解析 'llm.models' 的 LLM 配置失败: {e}")
                return None

        logger.warning(f"在命名空间 '{default_ns}' 中未找到键 'llm.models'。")
        return None

    def _load_system_limits(self) -> Dict[str, int]:
        """从配置中心加载系统级连接限制（system.config -> llm_service）"""
        default_ns = self._settings.local.apollo_namespace
        section = self._settings.get_section(default_ns)

        defaults = {
            "max_keepalive_connections": 20,
            "max_connections": 100
        }

        if not section:
            return defaults

        sys_config_raw = section.values.get("system.config")
        if not sys_config_raw:
            return defaults

        try:
            if isinstance(sys_config_raw, str):
                sys_config = json.loads(sys_config_raw)
            else:
                sys_config = sys_config_raw

            llm_service = sys_config.get("llm_service", {})
            conn_limits = llm_service.get("connection_limits", {})

            return {
                "max_keepalive_connections": conn_limits.get("max_keepalive_connections", 20),
                "max_connections": conn_limits.get("max_connections", 100)
            }
        except Exception as e:
            logger.warning(f"解析 system.config 中的连接限制失败，使用默认值: {e}")
            return defaults

    async def reload(self) -> None:
        """重新加载配置（热加载核心逻辑）"""
        # 1. 加锁
        async with self._reload_lock:
            # 2. 加载模型配置
            llm_config = self._load_llm_config()

            # 3. 加载连接池配置
            conn_limits = self._load_system_limits()

            # 4. 配置指纹机制
            current_signature = (
                llm_config.model_dump() if llm_config else None,
                conn_limits
            )

            # 5. 对比指纹
            if self._last_config_signature == current_signature:
                logger.debug("LLM 配置未发生变更，跳过重载。")
                return

            # 6. 记录重载日志
            logger.info("应用新的 LLM 配置...")

            # 7. 检查配置是否有效
            if not llm_config:
                return

            # 8. 更新默认模型别名
            self._default_alias = llm_config.default_model

            # 9. 构建新运行时字典
            new_runtimes: Dict[str, LLMRuntime] = {}
            old_runtimes = self._runtimes

            # 10. 遍历配置，构建每个模型的运行时实例
            for alias, model_config in llm_config.models.items():
                try:
                    # 构造运行时实例时，传入 self._event_bus
                    runtime = LLMRuntime(
                        alias=alias,
                        config=model_config,
                        connection_limits=conn_limits,
                        event_bus=self._event_bus
                    )
                    # 12. 添加到新字典
                    new_runtimes[alias] = runtime
                except Exception as e:
                    logger.error(f"构建运行时 '{alias}' 失败，跳过: {e}")

            # 14. 原子替换运行时字典
            self._runtimes = new_runtimes

            # 15. 更新配置指纹
            self._last_config_signature = current_signature

            # 16. 异步销毁旧运行时
            to_teardown = list(old_runtimes.values())
            for rt in to_teardown:
                await rt.teardown()

            # 17. 记录重载成功日志
            if new_runtimes:
                logger.success(
                    f"LLMManager 重新加载完成。活跃模型: {list(self._runtimes.keys())} | 连接池配置: {conn_limits}"
                )
