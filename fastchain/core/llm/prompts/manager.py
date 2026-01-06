from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import yaml
from jinja2 import Template
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from loguru import logger

from .schema import PromptTemplateConfig
from ...config.constants import ResourceName, ResourceKind
from ....core.config import SettingsManager
from ....core.resources.base import HealthStatus, Resource, ResourceHealth
from ....core.resources.events import Event, EventBus, EventConstants


class PromptManager(Resource):
    """Prompt 模板管理器（Resource）

    负责从配置中心加载和管理 Prompt 模板，支持热加载和 Jinja2 渲染

    核心特性：
    1. 前缀扫描策略：在配置 Namespace 中扫描所有以 `prefix` 开头的 Key
    2. 热加载：监听 Apollo 配置更新事件，自动重新加载模板
    3. 预编译：启动时将所有 Jinja2 模板预编译为 Template 对象，提升渲染性能
    4. 错误隔离：单个模板加载失败不影响其他模板，保证系统可用性
    5. 配置去重：智能识别配置变更，避免无关更新触发重载

    配置格式：
    - Key: "prompts.<template_id>"（例如 "prompts.coding_v1"）
    - Value: YAML 格式的 PromptTemplateConfig

    Strategy:
    ---------
    Prefix Scanning: 在默认配置 Namespace 中扫描所有以 `prefix` (默认 "prompts.") 开头的 Key。
    每个 Key 对应一个模板 ID，Value 是 YAML 格式的模板定义。

    Attributes:
        self._settings (SettingsManager): 配置管理器，用于读取 Apollo 配置
        self._event_bus (EventBus): 事件总线，用于监听配置更新
        self._prefix (str): 模板 Key 的前缀（默认 "prompts."）
        self._configs (Dict[str, PromptTemplateConfig]): 已加载的模板配置
        self._compiled (Dict[str, List[Dict[str, Any]]]): 预编译的 Jinja2 模板

    Methods:
        render_messages: 渲染模板，返回 BaseMessage 列表
        reload: 重新加载所有模板（支持热加载）
    """

    def __init__(
            self,
            settings: SettingsManager,
            event_bus: EventBus,
            prefix: str = "prompts."
    ) -> None:
        """初始化 PromptManager

        Args:
            settings (SettingsManager): 配置管理器
            event_bus (EventBus): 事件总线
            prefix (str): 模板 Key 的前缀。默认为 "prompts."
        """
        # 显式指定 startup_priority=20 (业务层)，这样它会等待 Priority 10 (DB层) 启动完成后再启动
        super().__init__(name=ResourceName.PROMPT, kind=ResourceKind.PROMPT, startup_priority=20)
        self._settings = settings
        self._event_bus = event_bus
        self._prefix = prefix

        # 存储已加载的配置和编译后的模板
        self._configs: Dict[str, PromptTemplateConfig] = {}
        self._compiled: Dict[str, List[Dict[str, Any]]] = {}

        self._started = False
        # 防止并发重载冲突
        self._lock = asyncio.Lock()

        # 用于缓存上次配置
        self._last_config_signature: Any = None

    async def start(self) -> None:
        """启动 PromptManager

        执行以下操作：
        1. 订阅配置更新事件（用于热加载）
        2. 初始加载所有模板

        Note:
            该方法是幂等的，重复调用不会产生副作用
        """
        if self._started:
            return

        logger.info(f"正在启动 PromptManager (扫描前缀='{self._prefix}')...")

        # 确保能正确接收到 ApolloResource 发出的标准配置更新事件
        self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        await self.reload()
        self._started = True

    async def stop(self) -> None:
        """停止 PromptManager

        清理所有已加载的模板，释放内存
        """
        self._started = False
        self._configs.clear()
        self._compiled.clear()

    async def health_check(self) -> ResourceHealth:
        """健康检查

        Returns:
            ResourceHealth: 包含状态和已加载模板数量的健康信息
        """
        status = HealthStatus.HEALTHY if self._started else HealthStatus.INIT
        return ResourceHealth(
            kind=self.kind,
            name=self.name,
            status=status,
            message=f"Loaded {len(self._compiled)} templates",
            details={"templates": list(self._compiled.keys())}
        )

    def render_messages(self, template_id: str, variables: Dict[str, Any]) -> List[BaseMessage]:
        """渲染模板，返回 BaseMessage 列表

        流程：
        1. 查找预编译的模板
        2. 合并默认变量和用户传入的变量
        3. 使用 Jinja2 渲染每条消息的内容
        4. 根据 role 类型构造对应的 BaseMessage 对象

        Args:
            template_id (str): 模板 ID（不含 prefix，例如 "coding_v1"）
            variables (Dict[str, Any]): 模板变量。会覆盖 default_variables 中的同名变量

        Returns:
            List[BaseMessage]: 渲染后的消息列表（可直接传递给 LLM）

        Raises:
            ValueError: 当模板 ID 不存在时。
            RuntimeError: 当渲染过程中发生错误时（例如缺少必需变量）
        """
        if template_id not in self._compiled:
            raise ValueError(f"找不到提示词模板 '{template_id}'。")

        config = self._configs[template_id]
        # 合并默认变量和用户传入的变量（用户变量优先级更高）
        final_vars = config.default_variables.copy()
        if variables:
            final_vars.update(variables)

        rendered_messages = []
        compiled_list = self._compiled[template_id]

        try:
            # 遍历每条消息，使用 Jinja2 渲染内容
            for item in compiled_list:
                role = item["role"]
                tpl: Template = item["template"]

                # Jinja2 渲染：将 {{ variable }} 替换为实际值
                text = tpl.render(**final_vars)

                # 根据 role 类型构造对应的 BaseMessage，这样可以确保 LLM 能正确识别消息的角色
                if role == "system":
                    rendered_messages.append(SystemMessage(content=text))
                elif role == "user":
                    rendered_messages.append(HumanMessage(content=text))
                elif role == "assistant":
                    rendered_messages.append(AIMessage(content=text))
                else:
                    # 未知 role 类型，默认作为 HumanMessage 处理
                    rendered_messages.append(HumanMessage(content=text))

        except Exception as e:
            # 渲染失败通常是因为缺少必需的变量，或者模板语法错误
            logger.error(f"渲染模板 '{template_id}' 时出错: {e}")
            raise RuntimeError(f"渲染提示词模板失败: {e}") from e

        return rendered_messages

    async def _on_config_updated(self, _event: Event) -> None:
        """配置更新事件处理器"""
        # 这里可以进一步优化：检查 _event.payload 中的 updated_namespaces 是否包含当前 namespace
        # 但考虑到 reload() 内部已经有指纹比对逻辑（_last_config_signature），重复调用开销很小，所以暂时直接调用 reload()
        logger.debug("检测到配置更新，正在检查 PromptManager 是否需要重载...")
        await self.reload()

    async def reload(self) -> None:
        """重新加载所有模板（支持热加载）

        流程：
        1. 提取 Prompt 相关的配置子集
        2. 对比配置指纹，如果未变则跳过
        3. 解析 YAML 格式的模板配置
        4. 预编译 Jinja2 模板
        5. 原子性替换旧的模板（保证并发安全）

        Note:
            该方法使用 asyncio.Lock 确保并发安全，防止多个配置更新事件同时触发重载
        """
        async with self._lock:
            default_ns = self._settings.local.apollo_namespace
            section = self._settings.get_section(default_ns)

            if not section:
                logger.warning(f"默认命名空间 '{default_ns}' 为空或未找到。")
                return

            # 1. 提取相关的配置子集 (只关心以 prefix 开头的)
            current_prompt_config = {
                k: v for k, v in section.values.items()
                if k.startswith(self._prefix)
            }

            # 2. 对比 (字典内容比较)
            if self._last_config_signature == current_prompt_config:
                logger.debug("Prompt 配置未发生变更，跳过重载。")
                return

            new_configs = {}
            new_compiled = {}

            # 扫描筛选后的配置
            for key, raw_value in current_prompt_config.items():
                # 提取 ID: prompts.coding_v1 -> coding_v1
                template_id = key[len(self._prefix):]

                try:
                    # 防御性检查：确保 Value 是字符串（Apollo 可能返回其他类型）
                    if not isinstance(raw_value, str):
                        logger.warning(f"跳过提示词键 '{key}' 的非字符串值")
                        continue

                    # A. 解析 YAML，使用 safe_load 防止 YAML 执行任意代码（安全考量）
                    data = yaml.safe_load(raw_value)
                    if not isinstance(data, dict):
                        logger.warning(f"'{key}' 的 YAML 结构无效（必须是字典类型）")
                        continue

                    # B. 验证并构造 PromptTemplateConfig，使用 Pydantic 确保配置格式正确
                    config = PromptTemplateConfig(**data)

                    # C. 预编译 Jinja2 模板，将每条消息的 content 编译为 Template 对象，避免每次渲染时重复编译
                    compiled_list = []
                    for msg in config.messages:
                        compiled_list.append({
                            "role": msg.role,
                            "template": Template(msg.content)
                        })

                    # 存储到临时字典（避免污染当前的配置）
                    new_configs[template_id] = config
                    new_compiled[template_id] = compiled_list

                except Exception as e:
                    # 单个模板加载失败，记录日志但不中断整个加载过程
                    logger.error(f"加载提示词模板 '{template_id}' 失败: {e}")

            # 原子性替换：确保并发读取时不会看到不一致的状态
            self._configs = new_configs
            self._compiled = new_compiled

            # 3. 更新指纹
            self._last_config_signature = current_prompt_config

            if new_configs:
                logger.success(
                    f"PromptManager 重新加载完成。已加载 {len(new_configs)} 个模板: {list(new_configs.keys())}")
            else:
                logger.info(f"PromptManager 重新加载完成（未找到前缀为 '{self._prefix}' 的模板）。")
