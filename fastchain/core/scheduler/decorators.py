from __future__ import annotations

import functools
from typing import List, Callable, Tuple, Any

from .types import JobConfig

# 全局任务注册表（暂存代码中定义的任务）
# 设计原因：装饰器在模块导入时就会执行，此时 SchedulerManager 尚未初始化，因此需要一个全局容器先收集所有被 @scheduled 标记的任务
# 等到 SchedulerManager 启动时再统一扫描并注册到 APScheduler
_REGISTRY: List[JobConfig] = []


def scheduled(
        key: str,
        cron: str,
        distributed: bool = True,
        enabled: bool = True,
        misfire_grace_time: int = 60
):
    """定时任务装饰器

    将普通的 Python 函数（同步或异步）标记为 FastChain 定时任务。支持自动注册到全局任务注册表、分布式锁标记和 Cron 表达式定义

    Args:
        key (str): 任务唯一标识（用于 Apollo 配置覆盖、日志追踪和分布式锁键名）。建议使用下划线分隔的小写字母，如 "daily_report"
        cron (str): Cron 表达式（分 时 日 月 周），遵循标准五段式格式。例如 "0 1 * * *" 表示每天凌晨 1 点执行
        distributed (bool): 是否开启分布式锁。默认为 True
            - True: 集群模式（依赖 Redis 锁），保证全集群同一时刻只有一个实例执行任务，适用于全局性任务（如生成报表、数据归档）
            - False: 本地模式，每个 Pod 都会独立执行，适合本地缓存刷新、健康检查等每个实例都需要独立处理的任务
        enabled (bool): 默认开启状态。默认为 True，可以在 Apollo 配置中心覆盖此值，实现动态启停
        misfire_grace_time (int): 错过执行时间的容忍窗口（秒）。默认为 60
            当任务因系统繁忙等原因错过预定执行时间时，APScheduler 在此时间窗口内仍会尝试执行，超过窗口则跳过本次执行。

    Returns:
        Callable: 装饰后的函数（保留原函数签名和行为）

    Note:
        - 该装饰器不强制要求函数为 async，同步函数也可以被装饰
          Manager 层会自动识别同步函数并使用 asyncio.to_thread 执行，避免阻塞事件循环
        - 装饰器在模块导入时即执行，因此所有任务会在应用启动前就注册到 _REGISTRY
        - 实际的调度逻辑由 SchedulerManager 负责，装饰器本身只做元数据收集
    """

    def decorator(func: Callable[..., Any]):
        # SchedulerManager 会自动识别同步函数并使用 asyncio.to_thread 执行，
        # 这样可以避免同步的重型任务（如大量磁盘 I/O）阻塞 asyncio 事件循环。

        # 将代码中的定义转换为 JobConfig 对象，这里 func 字段会被注入，方便后续调度时直接调用
        job_config = JobConfig(
            key=key,
            cron=cron,
            distributed=distributed,
            enabled=enabled,
            misfire_grace_time=misfire_grace_time,
            # 运行时注入函数对象引用
            func=func
        )

        # 注册到全局任务列表
        # 注意：此处是 append 而非 replace，因此同一个 key 多次装饰会导致重复注册，实际项目中应在 SchedulerManager 层做去重或抛出警告
        _REGISTRY.append(job_config)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # 装饰器的 wrapper 仅用于保持函数签名一致性，实际调度时不会经过这里
            # APScheduler 会直接调用 job_config.func，而不是这个 wrapper
            # 此处保留 async 包装是为了在极少数手动调用装饰后函数时保持异步接口
            return await func(*args, **kwargs)

        return wrapper

    return decorator


def get_registered_jobs() -> Tuple[JobConfig, ...]:
    """获取所有已注册的任务配置（只读视图）

    返回一个包含所有通过 @scheduled 装饰器注册的任务配置的元组。使用元组而非列表可以防止外部代码意外修改注册表

    Returns:
        Tuple[JobConfig, ...]: 所有已注册任务的配置对象元组（只读）

    Note:
        - 该函数由 SchedulerManager 在启动时调用，用于获取所有待调度的任务
        - 返回元组而非列表是为了提供不可变视图，避免外部代码修改全局注册表
        - 如果需要在运行时动态添加任务，应直接操作 SchedulerManager，而非修改 _REGISTRY
    """
    # 返回元组（不可变）以防止外部意外修改全局注册表
    return tuple(_REGISTRY)
