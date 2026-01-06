from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from loguru import logger

from .constants import (
    CONFIG_DIR_NAME,
    ENV_FILE_NAME,
    PROJECT_ROOT_MARKERS,
    PROJECT_ROOT_SEARCH_MAX_DEPTH,
    PROJECT_ROOT_SEARCH_MAX_DEPTH_ENV_VAR,
    LOG_DIR_NAME,
    DATA_DIR_NAME,
)

# 显式指定项目根目录的环境变量（最高优先级），适用于容器化部署或非标准项目结构场景
PROJECT_ROOT_ENV_VAR = "PROJECT_ROOT"


@dataclass(frozen=True)
class PathSettings:
    """项目路径配置（不可变数据类）

    本类用于封装项目运行时所需的所有关键路径信息，包括：
    - 项目根目录及项目名
    - 日志、数据、配置目录
    - 配置文件的完整路径

    设计考量：
    - 使用 dataclass(frozen=True) 保证不可变性，避免运行时误修改路径配置
    - 所有路径均为 pathlib.Path 对象，便于跨平台操作和路径拼接
    - 通过类方法 detect() 自动探测项目根目录，支持多种部署场景
    - 提供路径验证和目录创建方法，确保运行时环境准备就绪

    典型使用场景：
    - 应用启动时调用 PathSettings.detect() 获取路径配置
    - 在日志初始化、数据持久化、配置加载等模块中引用对应路径
    - 在测试环境中通过依赖注入传入 mock 路径配置

    Attributes:
        project_root (Path): 项目根目录的绝对路径
        project_name (str): 项目名称（根目录的文件夹名）
        log_dir (Path): 日志文件存储目录
        data_dir (Path): 数据文件存储目录
        config_dir (Path): 配置文件存储目录
        config_file (Path): 主配置文件（settings.toml）的完整路径

    Methods:
        detect: 自动探测项目根目录并构造 PathSettings 实例
        ensure_runtime_dirs: 确保运行时目录（log、data）存在
        validate_config_paths: 验证配置目录和配置文件是否存在
    """

    project_root: Path
    project_name: str
    log_dir: Path
    data_dir: Path
    config_dir: Path
    config_file: Path

    @staticmethod
    def _resolve_max_depth() -> int:
        """解析项目根目录搜索的最大深度。

        优先级：
        1. 环境变量 ROOT_SEARCH_MAX_DEPTH（如果存在且为正整数）
        2. 默认值 PROJECT_ROOT_SEARCH_MAX_DEPTH（10 层）

        Returns:
            int: 最大搜索深度（正整数）。

        Note:
            此方法设计为静态方法，因为它不依赖实例状态，且可能在类方法中被调用。
        """
        raw = os.getenv(PROJECT_ROOT_SEARCH_MAX_DEPTH_ENV_VAR)
        if raw is not None:
            try:
                value = int(raw)
                # 增加合理上限，防止过大导致 IO 问题
                if 0 < value <= 100:
                    return value
                logger.warning(f"无效的 ROOT_SEARCH_MAX_DEPTH: {value}，使用默认值。")
            except ValueError:
                # 环境变量值无效，静默回退到默认值，这里不抛异常是因为环境变量可能被误配置，回退更鲁棒
                pass
        return PROJECT_ROOT_SEARCH_MAX_DEPTH

    @staticmethod
    def _find_project_root() -> tuple[Path, str]:
        """自动探测项目根目录

        探测策略（优先级从高到低）：
        1. 环境变量 PROJECT_ROOT（显式指定，适用于容器化部署）
        2. 向上递归查找标记文件（.git、pyproject.toml），直到找到或达到最大深度
        3. 回退策略：使用当前工作目录 (CWD) 并告警

        设计考量：
        - 环境变量优先级最高，适用于非标准部署场景（如 Docker、Lambda）
        - 标记文件查找适用于本地开发和常规服务器部署
        - 回退策略确保在找不到标记文件时也能正常运行（虽然可能不准确）

        Returns:
            tuple[Path, str]: (项目根目录的绝对路径, 项目名称)

        Note:
            - 此方法可能触发文件系统 I/O，不应在性能敏感路径频繁调用
            - 建议在应用启动阶段调用一次，并通过 lru_cache 缓存结果
        """
        # 策略 1: 显式环境变量（最高优先级）
        explicit_root = os.getenv(PROJECT_ROOT_ENV_VAR)
        if explicit_root:
            path = Path(explicit_root).absolute()
            if path.exists() and path.is_dir():
                return path, path.name
            # 如果环境变量指定的路径不存在，继续尝试其他策略，这里不报错是为了在配置错误时仍能尝试自动探测

        # 策略 2: 向上递归查找标记文件
        current = Path(__file__).absolute().parent
        max_depth = PathSettings._resolve_max_depth()

        for _ in range(max_depth):
            # 防止符号链接循环 (简单防御：解析后比较)
            # Path.resolve() 默认会处理 symlink
            if current.name in [".", ".."]:
                break

            # 检查当前目录是否包含任何标记文件
            for marker in PROJECT_ROOT_MARKERS:
                if (current / marker).exists():
                    return current, current.name

            # 向上一级（parent）
            parent = current.parent
            if parent == current:
                # 已到达文件系统根目录，无法继续向上
                break
            current = parent

        # 策略 3: 安全回退到 CWD
        fallback = Path.cwd()
        logger.warning(
            f"无法自动检测项目根目录 (标记文件: {PROJECT_ROOT_MARKERS})。"
            f"回退到当前工作目录: {fallback}。"
            f"建议设置 {PROJECT_ROOT_ENV_VAR} 环境变量。"
        )
        return fallback, fallback.name

    @classmethod
    def detect(cls, *, validate_config: bool = False) -> "PathSettings":
        """自动探测并构造 PathSettings 实例

        此方法会：
        1. 调用 _find_project_root() 探测项目根目录
        2. 基于根目录推导出其他路径（log、data、config、config_file）
        3. 如果 validate_config=True，提前验证配置目录和文件是否存在

        Args:
            validate_config (bool): 是否在构造时验证配置文件存在性。默认为 False
                - True: 如果配置目录或文件不存在，立即抛出 FileNotFoundError
                - False: 延迟验证，适用于在应用启动后再手动调用 validate_config_paths()

        Returns:
            PathSettings: 包含所有路径信息的不可变配置对象

        Raises:
            FileNotFoundError: 当 validate_config=True 且配置目录或文件不存在时抛出

        Note:
            - 推荐在应用启动阶段调用此方法，并通过 lru_cache 缓存结果
            - 在测试环境中，建议通过依赖注入传入 mock 的 PathSettings 实例
        """
        # 探测项目根目录
        project_root, project_name = cls._find_project_root()

        # 推导其他路径（使用 pathlib 的 / 操作符进行路径拼接）
        log_dir = project_root / LOG_DIR_NAME
        data_dir = project_root / DATA_DIR_NAME
        config_dir = project_root / CONFIG_DIR_NAME
        config_file = config_dir / ENV_FILE_NAME

        # 可选的提前验证（适用于快速失败场景）
        if validate_config:
            if not config_dir.exists():
                raise FileNotFoundError(
                    f"配置目录 {config_dir} 不存在！"
                    f"请创建 {CONFIG_DIR_NAME} 目录，并放置 {ENV_FILE_NAME}。"
                )
            if not config_file.exists():
                raise FileNotFoundError(
                    f"配置文件 {config_file} 不存在！"
                    f"请确保 {ENV_FILE_NAME} 在 {CONFIG_DIR_NAME} 目录中。"
                )

        return cls(
            project_root=project_root,
            project_name=project_name,
            log_dir=log_dir,
            data_dir=data_dir,
            config_dir=config_dir,
            config_file=config_file,
        )

    def ensure_runtime_dirs(self) -> None:
        """确保运行时目录（log、data）存在

        此方法会创建 log_dir 和 data_dir（如果不存在），适用于应用启动阶段

        设计考量：
        - 使用 mkdir(parents=True, exist_ok=True) 确保幂等性
        - 不创建 config_dir，因为配置目录应由开发者手动维护
        - 不抛异常（除非文件系统权限不足），适合在启动脚本中调用

        Note:
            - 此方法可能触发文件系统写操作，需确保进程有足够权限
            - 在只读文件系统（如某些容器环境）中调用此方法会失败
        """
        for directory in (self.log_dir, self.data_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def validate_config_paths(self, *, create_config_dir: bool = False) -> None:
        """验证配置目录和配置文件是否存在

        此方法用于在应用启动时确保配置文件可用，避免在运行时才发现配置缺失

        Args:
            create_config_dir (bool): 如果配置目录不存在，是否自动创建。默认为 False
                - True: 自动创建配置目录（但不创建配置文件）
                - False: 配置目录不存在时抛出异常

        Raises:
            FileNotFoundError: 当配置目录或配置文件不存在时抛出

        Warning:
            - 即使 create_config_dir=True，此方法也不会自动创建配置文件
            - 这是为了防止应用带着空配置启动，导致难以排查的运行时错误
            - 建议在部署流程中显式检查配置文件存在性
        """
        if not self.config_dir.exists():
            if create_config_dir:
                # 自动创建配置目录（但仍需手动放置配置文件）
                self.config_dir.mkdir(parents=True, exist_ok=True)
            else:
                raise FileNotFoundError(
                    f"配置目录 {self.config_dir} 不存在！"
                    f"请创建 {CONFIG_DIR_NAME} 目录，并放置 {ENV_FILE_NAME}。"
                )

        # 无论是否自动创建目录，都必须验证配置文件存在性
        if not self.config_file.exists():
            raise FileNotFoundError(
                f"配置文件 {self.config_file} 不存在！"
                f"请确保 {ENV_FILE_NAME} 在 {CONFIG_DIR_NAME} 目录中。"
            )


@lru_cache(maxsize=1)
def get_path_settings() -> PathSettings:
    """获取路径配置单例（通过 lru_cache 缓存）

    设计考量：
    - 使用 @lru_cache(maxsize=1) 实现模块级单例，避免重复探测项目根目录
    - 相比全局变量，lru_cache 更加显式和可测试（可通过 cache_clear() 重置）
    - 适用于非测试环境的生产代码，测试时应通过依赖注入传入 mock 实例

    Returns:
        PathSettings: 缓存的路径配置实例。

    Note:
        - 此函数会在首次调用时触发文件系统探测（I/O 操作）
        - 后续调用直接返回缓存结果（O(1) 时间复杂度）
        - 在测试环境中，建议调用 get_path_settings.cache_clear() 清除缓存
    """
    return PathSettings.detect(validate_config=False)
