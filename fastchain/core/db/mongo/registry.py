from __future__ import annotations

import asyncio
import importlib
import inspect
import pkgutil
from pathlib import Path
from types import ModuleType
from typing import Dict, Type, List, Optional, Iterator, Set

from beanie import Document
from loguru import logger


class ModelRegistry:
    """Beanie 文档模型的自动发现与注册中心

    该类负责从指定的 Python 包中递归扫描所有继承自 beanie.Document 的模型类，并将它们注册到内部字典中，供后续 Beanie 初始化时使用

    核心功能：
    1. 异步并发扫描指定包及其子模块，提升大型项目的启动速度
    2. 自动过滤抽象基类和非模型类，只注册有效的 Document 子类
    3. 处理模型名称冲突，记录警告日志但允许覆盖（后加载的优先）
    4. 支持重复调用 discover_models，通过模块路径去重避免重复注册

    Attributes:
        _models (Dict[str, Type[Document]]): 模型名称到模型类的映射表。使用类名作为 key，可能存在命名冲突风险，需通过日志监控
        _processed_modules (Set[str]): 已处理的模块路径集合，用于去重和避免重复导入
        _import_semaphore (asyncio.Semaphore): 并发导入的信号量，限制同时进行的模块导入数量

    Methods:
        register: 手动注册单个模型类
        get_all_models: 获取所有已注册的模型类列表
        discover_models: 异步扫描指定包并自动注册所有模型

    Note:
        - 该类的设计假设模型类名在全局是唯一的，如果存在同名类，后注册的会覆盖先注册的
        - 信号量的值（10）是基于典型项目规模的经验值，对于超大型单体项目可能需要调整

    Warning:
        - 模块导入是同步阻塞操作，即使用 asyncio.to_thread 也无法完全消除 GIL 影响，因此在模块数量极多（数百个）时，启动时间仍可能较长
    """

    def __init__(self, concurrency: int = 10) -> None:
        """初始化模型注册中心。

        创建空的模型字典、模块处理记录集合，并初始化并发控制信号量。
        """
        self._models: Dict[str, Type[Document]] = {}
        self._processed_modules: Set[str] = set()
        # 信号量限制并发导入数量在apollo中配置，默认值20，防止因过度并发导致以下问题：
        # 1. 文件描述符耗尽（ulimit -n）
        # 2. I/O 子系统争用（大量磁盘读取）
        # 3. 内存峰值过高（模块加载时的临时对象）
        self._import_semaphore = asyncio.Semaphore(concurrency)

    def register(self, model_class: Type[Document]) -> Type[Document]:
        """手动注册一个 Beanie 文档模型类

        将模型类添加到内部注册表中，如果存在同名模型则覆盖并记录警告日志

        Args:
            model_class (Type[Document]): 要注册的 Beanie Document 子类

        Returns:
            Type[Document]: 返回传入的模型类本身，支持装饰器式调用

        Warning:
            如果存在同名模型，后注册的会覆盖先注册的，这可能导致难以追踪的 bug。建议在项目中确保所有模型类名全局唯一
        """
        name = model_class.__name__

        # 检测命名冲突：如果已有同名模型且不是同一个类对象，记录警告，这里使用身份比较（is），因为我们关心的是对象引用而非内容
        if name in self._models and self._models[name] != model_class:
            logger.warning(f"模型名称冲突：{name}。正在用 {model_class} 覆盖 {self._models[name]}")

        self._models[name] = model_class
        logger.debug(f"已注册 MongoDB 模型: {name}")
        return model_class

    def get_all_models(self) -> List[Type[Document]]:
        """获取所有已注册的模型类列表

        Returns:
            List[Type[Document]]: 所有已注册模型类的列表（顺序不保证）

        Note:
            返回的是列表副本（list()），而非字典的 values() 视图，避免在迭代过程中注册表被修改导致的 RuntimeError
        """
        return list(self._models.values())

    async def discover_models(self, package: str) -> List[Type[Document]]:
        """异步扫描指定包及其子模块，自动发现并注册所有 Beanie Document 模型

        执行流程：
        1. 导入根包（base package）
        2. 获取包的文件系统路径
        3. 使用 pkgutil.iter_modules 枚举所有子模块（不递归到子包）
        4. 并发导入所有子模块（受信号量限制）
        5. 对每个模块执行 inspect.getmembers 检查并注册符合条件的 Document 类
        6. 处理根包本身（因为模型可能直接定义在根包的 __init__.py 中）

        Args:
            package (str): 要扫描的 Python 包的完全限定名，例如 "fastchain.core.db.mongo.models"

        Returns:
            List[Type[Document]]: 本次扫描发现并注册的所有模型类列表

        Raises:
            Exception: 如果根包导入失败或扫描过程中发生意外错误，会重新抛出异常

        Note:
            - 该方法使用 asyncio.gather 并发处理多个子模块，提升大型项目的启动速度
            - 对于已处理过的模块，会通过 _processed_modules 集合去重，避免重复注册
            - 错误处理策略：子模块导入失败不会中断整体流程，但会记录错误日志。

        Warning:
            如果包路径不存在或包含语法错误的模块，部分模型可能无法注册，需通过日志监控确保所有预期模型都被成功发现
        """
        logger.info(f"正在扫描包 {package} 中的 MongoDB 模型")

        try:
            # 步骤 1: 导入根包
            # 这一步是同步的，因为 importlib.import_module 是阻塞操作
            # 使用 asyncio.to_thread 将其移到线程池中执行，避免阻塞事件循环
            base_module = await self._import_module(package)
            if not base_module:
                logger.warning(f"无法导入基础包 '{package}'，跳过模型发现。")
                return []

            # 步骤 2: 确定包的文件系统路径
            # __path__ 属性存在于包（package）中，指向包所在的目录列表
            # __file__ 属性存在于模块（module）中，指向模块文件的路径
            paths = []
            if hasattr(base_module, "__path__"):
                # 这是一个包，可能有多个路径（例如 namespace package）
                paths = list(base_module.__path__)
            elif hasattr(base_module, "__file__") and base_module.__file__:
                # 这是一个单文件模块，取其父目录
                paths = [str(Path(base_module.__file__).parent)]

            if not paths:
                # 无法确定路径，可能是内置模块或动态生成的模块
                return []

            # 步骤 3: 枚举所有子模块
            # pkgutil.iter_modules 只返回直接子模块，不递归到子包
            # 过滤掉子包（ispkg=True），只处理模块文件
            module_infos = []
            for path in paths:
                # 将路径转为 Path 对象，确保跨平台兼容性
                module_infos.extend(list(self._get_module_iterator(Path(path))))

            # 步骤 4: 并发导入所有子模块
            # 这里使用 asyncio.gather 实现并发，每个子模块的导入都是一个独立的协程任务
            # return_exceptions=True 确保单个模块导入失败不会中断整体流程
            tasks = [
                self._process_module(f"{package}.{module_info.name}")
                for module_info in module_infos
            ]

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # 检查是否有异常发生，记录到日志中
                for res in results:
                    if isinstance(res, Exception):
                        logger.error(f"模型发现过程中出错: {res}")

            # 步骤 5: 处理根包本身
            # 有些项目会直接在根包的 __init__.py 中定义模型类
            # 因为 _register_document_classes 是同步方法，所以直接调用即可
            self._register_document_classes(base_module)

            # 获取最终注册的所有模型
            models = self.get_all_models()
            logger.success(f"发现 {len(models)} 个 MongoDB 模型: {[m.__name__ for m in models]}")
            return models

        except Exception as e:
            logger.error(f"包 '{package}' 的模型发现失败: {e}")
            raise

    async def _import_module(self, module_path: str) -> Optional[ModuleType]:
        """异步导入指定的 Python 模块

        使用信号量限制并发导入数量，避免资源争用。将同步的 importlib.import_module 调用移到线程池中执行，以免阻塞事件循环

        Args:
            module_path (str): 模块的完全限定名，例如 "fastchain.core.db.mongo.models.user"

        Returns:
            Optional[ModuleType]: 成功导入返回模块对象，失败返回 None

        Note:
            - 使用 asyncio.to_thread 将阻塞的导入操作移到线程池，但由于 GIL 的存在，实际并发能力受限，主要收益是避免阻塞主事件循环
            - 信号量限制同时进行的导入数量，防止线程池耗尽或 I/O 瓶颈

        Warning:
            如果模块存在循环导入或语法错误，导入会失败，但不会中断整体流程
        """
        async with self._import_semaphore:
            try:
                # 将同步的 importlib.import_module 移到线程池中执行，这样可以避免在单个模块导入耗时较长时阻塞事件循环
                return await asyncio.to_thread(importlib.import_module, module_path)
            except ImportError as e:
                # ImportError 通常意味着模块不存在或依赖缺失
                logger.error(f"导入模块 '{module_path}' 失败: {e}")
                return None
            except Exception as e:
                # 捕获其他异常（如语法错误、循环导入等）
                logger.error(f"导入 '{module_path}' 时发生未预期的错误: {e}")
                return None

    async def _process_module(self, module_path: str) -> None:
        """处理单个模块：导入模块并注册其中的 Document 类

        该方法会检查模块是否已被处理过（通过 _processed_modules 集合），避免重复导入和注册

        Args:
            module_path (str): 模块的完全限定名

        Note:
            - 去重逻辑基于模块路径字符串，时间复杂度为 O(1)
            - 即使模块导入失败，也会被标记为已处理，避免无限重试
        """
        # 去重检查：如果模块已被处理，直接返回，使用集合查找的 O(1) 时间复杂度，在模块数量较多时效率高
        if module_path in self._processed_modules:
            return

        # 异步导入模块
        module = await self._import_module(module_path)
        if module:
            # 同步注册模块中的所有 Document 类
            # 因为 inspect.getmembers 和类型检查都是 CPU 密集型操作，
            # 且不涉及 I/O，所以保持同步调用即可
            self._register_document_classes(module)

            # 标记为已处理，即使注册过程中出现异常也要标记，避免无限重试
            self._processed_modules.add(module_path)

    def _register_document_classes(self, module: ModuleType) -> None:
        """扫描模块中的所有成员，注册符合条件的 Beanie Document 类

        该方法使用 inspect.getmembers 遍历模块的所有属性，对每个属性进行类型检查，只有满足以下条件的类才会被注册：
        1. 是一个类（inspect.isclass）
        2. 是 beanie.Document 的子类
        3. 不是 beanie.Document 本身（避免注册基类）
        4. 不是抽象基类（通过 Settings.is_root 标记识别）

        Args:
            module (ModuleType): 要扫描的 Python 模块对象

        Note:
            - inspect.getmembers 会返回模块中所有的属性，包括导入的类和函数，因此需要严格的类型检查和过滤
            - 错误处理策略：如果模块的某个成员访问失败（如属性是动态生成的），会记录警告但不中断整体流程
        """
        try:
            # inspect.getmembers 返回 (name, value) 的列表
            # 只关心 value，所以用 _ 忽略 name
            for _, obj in inspect.getmembers(module):
                # 检查是否是有效的 Document 类
                if self._is_valid_document_class(obj):
                    self.register(obj)
        except Exception as e:
            # 捕获所有异常，避免单个模块的问题影响整体扫描
            logger.warning(f"检查模块 {module.__name__} 时出错: {e}")

    @staticmethod
    def _is_valid_document_class(obj: Type) -> bool:
        """检查给定对象是否是有效的 Beanie Document 模型类

        有效的 Document 类需要满足以下条件：
        1. 是一个类对象（而非函数、模块等）
        2. 是 beanie.Document 的子类
        3. 不是 beanie.Document 基类本身
        4. 不是抽象基类（通过 Settings.is_root 标记识别）

        Args:
            obj (Type): 要检查的对象

        Returns:
            bool: 如果是有效的 Document 类返回 True，否则返回 False

        Note:
            - Settings.is_root 是 Beanie 的内部约定，用于标记抽象基类或根模型
            - 该检查顺序经过优化：先进行轻量级检查（isclass），后进行重量级检查（issubclass），减少不必要的类型检查开销
        """
        # 检查 1: 是否是类对象，这是最快的检查，可以过滤掉大部分非类对象（函数、变量等）
        if not inspect.isclass(obj):
            return False

        # 检查 2: 是否是 Document 的子类
        # issubclass 会进行 MRO（方法解析顺序）遍历，相对耗时，所以放在 isclass 之后
        if not issubclass(obj, Document):
            return False

        # 检查 3: 不是 Document 基类本身，避免将 Beanie 的基类也注册进来
        if obj == Document:
            return False

        # 检查 4: 不是抽象基类（通过 Settings.is_root 标记），这些基类不应该被 Beanie 初始化，只有具体的业务模型才需要
        if hasattr(obj, "Settings"):
            settings = getattr(obj, "Settings")
            if inspect.isclass(settings) and getattr(settings, "is_root", False):
                return False

        return True

    @classmethod
    def _get_module_iterator(cls, package_path: Path) -> Iterator:
        """获取指定包路径下所有子模块的迭代器

        使用 pkgutil.iter_modules 扫描包目录，只返回模块文件（.py），过滤掉子包（子目录）

        Args:
            package_path (Path): 包的文件系统路径

        Returns:
            Iterator: 返回 ModuleInfo 对象的迭代器

        Note:
            - pkgutil.iter_modules 只扫描一层，不递归到子包，因此性能较好
            - 过滤掉子包（ispkg=True）的原因是：子包会在后续递归中单独处理，这里只需要处理当前层级的模块文件
        """
        return (
            module_info for module_info in pkgutil.iter_modules([str(package_path)])
            # 只处理模块文件，不处理子包
            if not module_info.ispkg
        )
