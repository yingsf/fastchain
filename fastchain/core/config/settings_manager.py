from __future__ import annotations

from typing import Any, Optional, Callable

from .local_settings import LocalSettings, load_local_settings
from .path_settings import PathSettings, get_path_settings
from .runtime_store import (
    ConfigSection,
    GroupKey,
    RuntimeConfigStore,
    RuntimeConfigSnapshot,
)


class SettingsManager:
    """顶层配置访问门面（聚合本地配置、运行时配置和路径配置）

    本类是配置管理系统的统一入口点，聚合三种配置源：
    - LocalSettings: 本地基础配置（基于 Dynaconf 从 settings.toml 加载）
    - RuntimeConfigStore: 运行时配置仓库（由 Apollo/Nacos 等配置中心热更新）
    - PathSettings: 项目路径信息（项目根目录、日志目录、配置目录等）

    设计理念：
    - **统一接口**：提供统一的配置访问接口，隔离配置来源的差异
    - **只读保证**：本地配置和路径配置通过 @property 暴露为只读属性
    - **热更新支持**：运行时配置通过 RuntimeConfigStore 支持原子更新
    - **配置转换**：支持注册转换器，自动将配置字符串转换为 Python 对象

    Attributes:
        _local (LocalSettings): 本地基础配置（只读）
        _runtime_store (RuntimeConfigStore): 运行时配置仓库（可热更新）
        _paths (PathSettings): 项目路径信息（只读）

    Methods:
        local: 访问本地基础配置
        paths: 访问项目路径信息
        runtime_store: 访问运行时配置仓库
        get_section: 获取指定分组的完整配置
        get_value: 获取指定分组下的单个配置项
        get_snapshot: 获取当前完整运行时配置快照
        register_transformer: 注册配置转换器
    """

    def __init__(
            self,
            *,
            local_settings: Optional[LocalSettings] = None,
            runtime_store: Optional[RuntimeConfigStore] = None,
            path_settings: Optional[PathSettings] = None
    ) -> None:
        """初始化配置管理器

        Args:
            local_settings (LocalSettings | None): 本地基础配置（可选）
                默认为 None（自动加载）
            runtime_store (RuntimeConfigStore | None): 运行时配置仓库（可选）
                默认为 None（自动创建）
            path_settings (PathSettings | None): 项目路径信息（可选）
                默认为 None（自动获取）

        Note:
            支持依赖注入，方便单元测试时注入 Mock 对象
        """
        # 加载或注入本地配置
        self._local = local_settings or load_local_settings()

        # 创建或注入运行时配置仓库
        self._runtime_store = runtime_store or RuntimeConfigStore()

        # 获取或注入项目路径信息
        self._paths = path_settings or get_path_settings()

    # ========== 本地配置访问 ==========

    @property
    def local(self) -> LocalSettings:
        """访问本地基础配置（只读）

        Returns:
            LocalSettings: 本地配置对象

        Note:
            本地配置在应用启动时加载，运行期间不会改变，因此提供只读访问
        """
        return self._local

    # ========== 路径配置访问 ==========

    @property
    def paths(self) -> PathSettings:
        """访问项目路径信息（只读）

        Returns:
            PathSettings: 路径配置对象

        Note:
            路径配置在应用启动时确定，运行期间不会改变，因此提供只读访问
        """
        return self._paths

    # ========== 运行时配置访问 ==========

    @property
    def runtime_store(self) -> RuntimeConfigStore:
        """访问运行时配置仓库（可热更新）

        Returns:
            RuntimeConfigStore: 运行时配置仓库对象

        Note:
            运行时配置支持热更新（由配置中心推送），
            因此需要通过 RuntimeConfigStore 访问以获取最新配置
        """
        return self._runtime_store

    def get_section(self, group: GroupKey) -> Optional[ConfigSection]:
        """获取指定分组的完整配置

        Args:
            group (GroupKey): 分组键（如 "application"）

        Returns:
            ConfigSection | None: 配置分组对象，不存在则返回 None

        Note:
            ConfigSection 是不可变对象，提供只读视图，防止意外修改
        """
        return self._runtime_store.get_section(group)

    def get_value(
            self,
            group: GroupKey,
            key: str,
            default: Any = None
    ) -> Any:
        """获取指定分组下的单个配置项

        Args:
            group (GroupKey): 分组键（如 "application"）
            key (str): 配置项键名（如 "system.config"）
            default (Any): 默认值（配置项不存在时返回）默认为 None

        Returns:
            Any: 配置项的值（已通过转换器转换），不存在则返回默认值

        Note:
            如果为该配置项注册了转换器，返回的值将是转换后的对象（如 dict），
            而非原始字符串这是配置管理的核心改进点
        """
        return self._runtime_store.get_value(group, key, default)

    def get_snapshot(self) -> RuntimeConfigSnapshot:
        """获取当前完整运行时配置快照

        Returns:
            RuntimeConfigSnapshot: 配置快照对象（不可变）

        Note:
            快照是配置在某一时刻的原子视图，适用于需要一致性读取的场景
            （如批量配置解析、配置对比等）
        """
        return self._runtime_store.get_snapshot()

    def register_transformer(self, group: GroupKey, key: str, func: Callable[[Any], Any]) -> None:
        """注册配置转换器（代理到 RuntimeConfigStore）

        用于在配置存入仓库前，自动对原始字符串进行解析（如 JSON/YAML 反序列化）

        Args:
            group (GroupKey): 分组键（如 "application"）
            key (str): 配置项键名（如 "system.config"）
            func (Callable[[Any], Any]): 转换函数（如 json.loads）

        Note:
            转换器在配置更新时自动执行（在 Schema 校验之前），
            确保业务代码拿到的配置始终是已转换的对象，无需重复解析
            转换器注册应在应用启动早期完成（如 FastChainApp.__init__），
            确保在任何配置读取之前生效
        """
        self._runtime_store.register_transformer(group, key, func)


def create_settings_manager(
        *,
        local_settings: Optional[LocalSettings] = None,
        runtime_store: Optional[RuntimeConfigStore] = None,
        path_settings: Optional[PathSettings] = None
) -> SettingsManager:
    """创建 SettingsManager 实例（工厂函数）

    Args:
        local_settings (LocalSettings | None): 本地基础配置（可选）
        runtime_store (RuntimeConfigStore | None): 运行时配置仓库（可选）
        path_settings (PathSettings | None): 项目路径信息（可选）

    Returns:
        SettingsManager: 配置管理器实例

    Note:
        工厂函数简化了实例创建，支持依赖注入和单元测试
    """
    return SettingsManager(
        local_settings=local_settings,
        runtime_store=runtime_store,
        path_settings=path_settings,
    )
