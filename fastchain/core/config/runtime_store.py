from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Type, Union, Callable

from loguru import logger
from pydantic import BaseModel, ValidationError

from .enums import ConfigGroup

# 分组键类型别名：可以是字符串或 ConfigGroup 枚举值
GroupKey = Union[str, ConfigGroup]

# 转换器函数类型：接收原始值，返回转换后的值
TransformerFunc = Callable[[Any], Any]


@dataclass(frozen=True)
class ConfigSection:
    """单个逻辑配置分组的视图（不可变数据类）

    封装配置分组的键值对，提供只读访问接口

    Attributes:
        _values (Mapping[str, Any]): 配置键值对映射（私有属性）

    Properties:
        values: 返回只读映射视图（防止外部修改）

    Note:
        使用 frozen=True 确保实例创建后不可修改，
        配合 MappingProxyType 提供双层不可变保证
    """
    _values: Mapping[str, Any] = field(default_factory=dict)

    @property
    def values(self) -> Mapping[str, Any]:
        """返回只读映射视图

        Returns:
            Mapping[str, Any]: 配置键值对只读视图

        Note:
            使用 MappingProxyType 包装，防止外部代码意外修改配置
            这是不可变设计的关键：即使拿到了引用，也无法修改内容
        """
        if isinstance(self._values, dict):
            return MappingProxyType(self._values)
        return self._values


@dataclass(frozen=True)
class RuntimeConfigSnapshot:
    """整个运行时配置的一致性快照（不可变数据类）

    封装某一时刻所有配置分组的完整状态，支持一致性读取和版本管理

    Attributes:
        _sections (Mapping[str, ConfigSection]): 配置分组映射（私有属性）
        version (str | None): 配置版本号（可选）
        source (str | None): 配置来源标识（可选，如 "apollo", "cache"）

    Properties:
        sections: 返回只读映射视图（防止外部修改）

    Note:
        快照是配置管理的核心抽象：
        - 不可变性保证线程安全和一致性读取
        - 版本号支持配置变更追踪和回滚
        - 来源标识用于审计和问题排查
    """
    _sections: Mapping[str, ConfigSection] = field(default_factory=dict)
    version: Optional[str] = None
    source: Optional[str] = None

    @property
    def sections(self) -> Mapping[str, ConfigSection]:
        """返回只读映射视图

        Returns:
            Mapping[str, ConfigSection]: 配置分组只读视图

        Note:
            双层不可变保证：外层 MappingProxyType + 内层 frozen dataclass
        """
        if isinstance(self._sections, dict):
            return MappingProxyType(self._sections)
        return self._sections


class RuntimeConfigStore:
    """运行时配置仓库（支持热更新、原子切换、自动转换与 Schema 校验）

    核心升级：
    - 支持注册 Key-Level Transformer：在配置存入快照前，自动对特定 Key 的值进行转换（如 JSON 反序列化）
      这解决了 Apollo 只能存储字符串，而业务代码需要复杂对象（Dict/List）的问题

    设计理念：
    - 原子更新：使用异步锁保证配置更新的原子性和线程安全
    - 不可变快照：配置快照不可修改，读取时无需加锁，性能最优
    - 转换 + 校验：配置更新流水线：原始值 -> 转换 -> 校验 -> 存储
    - 容错降级：转换或校验失败时，保留旧配置或丢弃无效配置，避免系统崩溃

    Attributes:
        _snapshot (RuntimeConfigSnapshot): 当前配置快照（原子切换）
        _lock (asyncio.Lock): 异步锁（保护配置更新）
        _validators (Dict[str, Type[BaseModel]]): Pydantic 校验器映射
        _transformers (Dict[str, Dict[str, TransformerFunc]]): 转换器映射
            {section: {key: transformer}}

    Methods:
        get_snapshot: 获取当前配置快照
        get_section: 获取指定分组配置
        get_value: 获取指定配置项
        register_validator: 注册 Pydantic Schema 验证器
        register_transformer: 注册配置转换器
        replace_snapshot: 替换整个配置快照（原子操作）
        update_sections: 更新配置分组（支持转换和校验）
    """

    def __init__(self) -> None:
        """初始化运行时配置仓库

        Note:
            初始化为空快照，等待配置中心推送首次配置
        """
        # 当前配置快照（初始为空）
        self._snapshot: RuntimeConfigSnapshot = RuntimeConfigSnapshot()

        # 异步锁：保护配置更新操作的原子性
        self._lock = asyncio.Lock()

        # Pydantic 校验器注册表：{section_key: ModelClass}
        self._validators: Dict[str, Type[BaseModel]] = {}

        # 转换器注册表：{section_key: {item_key: transformer_func}}
        # 用于在配置存入前自动转换原始值（如 JSON 字符串 -> dict）
        self._transformers: Dict[str, Dict[str, TransformerFunc]] = {}

    def get_snapshot(self) -> RuntimeConfigSnapshot:
        """获取当前配置快照

        Returns:
            RuntimeConfigSnapshot: 当前配置快照（不可变对象）

        Note:
            快照是不可变对象，读取无需加锁，性能最优
            即使在配置更新期间读取，也能保证读到一致的状态
        """
        return self._snapshot

    def _normalize_group_key(self, key: GroupKey) -> str:
        """规范化分组键（将枚举转为字符串）

        Args:
            key (GroupKey): 分组键（字符串或枚举）

        Returns:
            str: 规范化后的字符串键

        Note:
            支持枚举和字符串两种键类型，提升 API 易用性
        """
        return key.value if isinstance(key, ConfigGroup) else str(key)

    def get_section(self, key: GroupKey) -> Optional[ConfigSection]:
        """获取指定分组的完整配置

        Args:
            key (GroupKey): 分组键（如 "application"）

        Returns:
            ConfigSection | None: 配置分组对象，不存在则返回 None
        """
        section_key = self._normalize_group_key(key)
        return self._snapshot.sections.get(section_key)

    def get_value(self, key: GroupKey, item: str, default: Any = None) -> Any:
        """获取指定分组下的单个配置项

        Args:
            key (GroupKey): 分组键（如 "application"）
            item (str): 配置项键名（如 "system.config"）
            default (Any): 默认值（配置项不存在时返回）默认为 None

        Returns:
            Any: 配置项的值（已通过转换器转换），不存在则返回默认值

        Note:
            如果为该配置项注册了转换器，返回的值将是转换后的对象
        """
        section = self.get_section(key)
        if section is None:
            return default
        return section.values.get(item, default)

    def register_validator(self, key: GroupKey, model_cls: Type[BaseModel]) -> None:
        """注册 Pydantic Schema 验证器

        为指定配置分组注册 Pydantic 模型类，用于在配置更新时进行 Schema 校验

        Args:
            key (GroupKey): 分组键（如 "application"）
            model_cls (Type[BaseModel]): Pydantic 模型类

        Note:
            重复注册会覆盖旧的校验器（记录警告日志）
        """
        section_key = self._normalize_group_key(key)

        # 检查是否重复注册
        if section_key in self._validators:
            logger.warning(
                f"覆盖校验器 '{section_key}': {self._validators[section_key].__name__} -> {model_cls.__name__}"
            )

        self._validators[section_key] = model_cls
        logger.debug(f"已注册校验器: '{section_key}'")

    def register_transformer(self, key: GroupKey, item: str, func: TransformerFunc) -> None:
        """为指定配置项注册转换器（如 json.loads）

        在配置更新时，会先调用转换器处理原始值，再进行 Schema 校验和存储

        Args:
            key (GroupKey): 分组键（如 "application"）
            item (str): 配置项键名（如 "system.config"）
            func (TransformerFunc): 转换函数（如 json.loads）

        Note:
            转换器的执行时机是在配置存入快照之前，而非每次读取时转换，
            确保转换只发生一次，性能最优
            转换失败时会记录错误日志并保留原始值，交给后续校验器处理
        """
        section_key = self._normalize_group_key(key)

        # 初始化该分组的转换器字典
        if section_key not in self._transformers:
            self._transformers[section_key] = {}

        # 注册转换器
        self._transformers[section_key][item] = func
        logger.debug(f"已注册转换器: '{section_key}.{item}' -> {func.__name__}")

    async def replace_snapshot(self, snapshot: RuntimeConfigSnapshot) -> None:
        """替换整个配置快照（原子操作）

        Args:
            snapshot (RuntimeConfigSnapshot): 新的配置快照

        Note:
            使用异步锁保证原子性，防止配置更新期间的竞态条件
        """
        async with self._lock:
            self._snapshot = snapshot

    def _prepare_new_sections_dict(
            self, old_snapshot: RuntimeConfigSnapshot, merge: bool
    ) -> Dict[str, ConfigSection]:
        """准备新配置分组字典

        Args:
            old_snapshot (RuntimeConfigSnapshot): 旧配置快照
            merge (bool): 是否合并模式（True: 保留旧配置；False: 完全替换）

        Returns:
            Dict[str, ConfigSection]: 新配置分组字典

        Note:
            合并模式下，未更新的配置分组将保留（增量更新）
            完全替换模式下，未更新的配置分组将被删除（全量更新）
        """
        return dict(old_snapshot.sections) if merge else {}

    def _apply_transformers(self, section_key: str, raw_data: Mapping[str, Any]) -> Dict[str, Any]:
        """执行配置转换逻辑

        遍历该分组下所有注册的转换器，对匹配的配置项进行转换

        Args:
            section_key (str): 配置分组键
            raw_data (Mapping[str, Any]): 原始配置数据

        Returns:
            Dict[str, Any]: 转换后的配置数据

        Note:
            转换逻辑的容错机制：
            - 只转换存在且值不为 None 的配置项
            - 跳过已经是对象的配置项（如本地文件加载的配置）
            - 转换失败时保留原始值，记录错误日志，交给后续校验器处理
        """
        # 复制一份数据，避免修改原始引用
        data = dict(raw_data)

        # 获取该分组下的所有转换器
        transformers = self._transformers.get(section_key)
        if not transformers:
            return data

        # 遍历所有转换器，执行转换
        for item_key, func in transformers.items():
            # 只有当配置中包含该 Key 且值不为 None 时才转换
            if item_key in data and data[item_key] is not None:
                original_val = data[item_key]

                # 如果已经是通过其他方式（如本地文件）加载好的对象，则跳过
                # 这里主要针对 Apollo 返回的字符串进行处理
                if not isinstance(original_val, str):
                    continue

                try:
                    # 执行转换
                    data[item_key] = func(original_val)
                except Exception as e:
                    # 转换失败时保留原始值，记录错误
                    logger.error(
                        f"配置转换失败 '{section_key}.{item_key}': {e}保留原始值"
                    )
                    # 转换失败时保留原始值，交给后续的 Validator 去报错（或者容忍）

        return data

    def _process_section_update(
            self,
            section_key: str,
            raw_data: Mapping[str, Any],
            old_snapshot: RuntimeConfigSnapshot,
            new_sections: Dict[str, ConfigSection],
            merge: bool,
            source: Optional[str]
    ) -> None:
        """处理单个分组的更新：转换 -> 校验 -> 存储

        配置更新的三阶段流水线：
        1. Transformation: 使用注册的转换器转换原始值（如 JSON 字符串 -> dict）
        2. Validation: 使用 Pydantic 模型校验转换后的数据（如类型、必填项检查）
        3. Storage: 将校验通过的配置存入新快照

        Args:
            section_key (str): 配置分组键
            raw_data (Mapping[str, Any]): 原始配置数据
            old_snapshot (RuntimeConfigSnapshot): 旧配置快照
            new_sections (Dict[str, ConfigSection]): 新配置分组字典（输出参数）
            merge (bool): 是否合并模式
            source (str | None): 配置来源标识

        Note:
            转换和校验失败时的容错策略：
            - 如果旧快照中存在该分组，保留旧配置（降级策略）
            - 如果旧快照中不存在该分组，丢弃无效配置（fail-fast）
        """
        # 1. 应用转换器
        # 将 "system.config": "{...}" 转换为 "system.config": {...}
        processed_data = self._apply_transformers(section_key, raw_data)

        # 2. Schema 校验
        validator = self._validators.get(section_key)
        if validator:
            try:
                # Pydantic 校验是基于转换后的数据的
                validated_obj = validator(**processed_data)

                # 将 Pydantic 模型转换为普通字典（存入快照）
                clean_data = validated_obj.model_dump(mode="json")
                new_sections[section_key] = ConfigSection(_values=clean_data)
            except (ValidationError, TypeError) as e:
                # 校验失败，执行容错逻辑
                self._handle_validation_failure(
                    section_key, old_snapshot, new_sections, merge, source, e
                )
        else:
            # 无校验器，直接使用转换后的数据
            new_sections[section_key] = ConfigSection(_values=processed_data)

    def _handle_validation_failure(
            self, section_key: str, old_snapshot: RuntimeConfigSnapshot,
            new_sections: Dict[str, ConfigSection], merge: bool,
            source: Optional[str], error: Exception
    ) -> None:
        """处理配置校验失败的容错逻辑

        Args:
            section_key (str): 配置分组键
            old_snapshot (RuntimeConfigSnapshot): 旧配置快照
            new_sections (Dict[str, ConfigSection]): 新配置分组字典
            merge (bool): 是否合并模式
            source (str | None): 配置来源标识
            error (Exception): 校验异常对象

        Note:
            容错策略：
            - 如果旧快照中存在该分组，保留旧配置（降级，保证系统可用）
            - 如果旧快照中不存在该分组，丢弃无效配置（fail-fast，避免引入脏数据）
        """
        logger.error(f"配置校验失败 '{section_key}' (来源: {source}): {error}")

        # 如果旧快照中存在该分组，保留旧配置
        if section_key in old_snapshot.sections:
            logger.warning(f"保留分组 '{section_key}' 的旧配置")
            if not merge:
                new_sections[section_key] = old_snapshot.sections[section_key]
        else:
            # 如果旧快照中不存在该分组，丢弃无效配置
            logger.critical(f"丢弃无效配置 '{section_key}'！")
            new_sections.pop(section_key, None)

    async def update_sections(
            self,
            sections: Mapping[GroupKey, Mapping[str, Any]],
            *,
            version: Optional[str] = None,
            source: Optional[str] = None,
            merge: bool = True
    ) -> None:
        """更新配置分组（支持转换和校验）

        工作流程：
        1. 参数校验
        2. 加锁（保证原子性）
        3. 准备新配置分组字典（合并或完全替换）
        4. 遍历所有分组，执行转换 -> 校验 -> 存储流水线
        5. 构建新快照并原子替换

        Args:
            sections (Mapping[GroupKey, Mapping[str, Any]]): 配置分组映射
            version (str | None): 配置版本号（可选）
            source (str | None): 配置来源标识（可选）
            merge (bool): 是否合并模式（True: 增量更新；False: 全量替换）
                默认为 True

        Raises:
            TypeError: 如果 sections 参数类型错误

        Note:
            原子更新保证：
            - 使用异步锁防止并发更新导致的竞态条件
            - 所有分组更新完成后才替换快照，确保一致性
            并发安全：
            - 更新期间读取操作仍可访问旧快照（不阻塞读）
            - 更新完成后原子切换到新快照（读操作立即看到新配置）
        """
        # 参数校验：确保 sections 是 Mapping 类型
        if sections is None or not isinstance(sections, Mapping):
            raise TypeError(f"update_sections expected Mapping, got {type(sections)}")

        async with self._lock:
            # 获取旧快照
            old = self._snapshot

            # 准备新配置分组字典
            new_sections = self._prepare_new_sections_dict(old, merge)

            # 遍历所有分组，执行转换 -> 校验 -> 存储流水线
            for group_key, raw_data in sections.items():
                sk = self._normalize_group_key(group_key)
                self._process_section_update(sk, raw_data, old, new_sections, merge, source)

            # 构建新快照
            self._snapshot = RuntimeConfigSnapshot(
                _sections=new_sections,
                # 版本号：新版本 > 旧版本
                version=version or old.version,
                # 来源：新来源 > 旧来源
                source=source or old.source
            )
