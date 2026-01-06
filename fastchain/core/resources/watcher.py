from __future__ import annotations

import asyncio
import random
import time
from abc import ABC, abstractmethod
from typing import Any, Final

from .events import EventBus
from .limits import ResourceLimiter

# 默认 tick 超时时间（单位：秒），避免子类在 tick() 中写出死循环或长时间阻塞，导致任务积压
DEFAULT_TICK_TIMEOUT: Final[float] = 60.0

# 停止操作的最大等待时间（单位：秒），避免优雅关闭时无限等待，导致进程无法退出
STOP_TIMEOUT: Final[float] = 5.0


class BackgroundWatcher(ABC):
    """通用后台任务观察者（基于 Template Method 模式）

    该类提供了一个可扩展的后台任务框架，用于实现周期性轮询任务（如健康检查、指标采集、资源监控等）
    通过 Template Method 模式，将任务调度逻辑固化在基类中，将具体业务逻辑委托给子类实现

    增强特性：
    - 集成 ResourceLimiter：原生支持资源限流，防止轮询任务过载
    - 自动装配 EventBus：自动将事件总线注入限流器，激活可观测性（限流告警）

    核心特性：
    - Template Method 模式：基类定义任务调度框架，子类实现业务逻辑
    - 自适应退避重试：失败后自动增加间隔，避免雪崩效应
    - 抖动机制：随机化轮询间隔，避免多个实例同时执行（防止惊群）
    - 超时保护：单次 tick 超时自动中断，避免任务积压
    - 优雅关闭：多层保护机制，确保任务能正常退出
    - 热重载支持：动态更新轮询间隔，无需重启

    Attributes:
        _name (str): 观察者名称，用于日志和任务标识
        _logger (Any): 日志记录器（通常是 logging.Logger）
        _min_interval (float): 最小轮询间隔（单位：秒）
        _max_interval (float): 最大轮询间隔（单位：秒）
        _jitter (float): 抖动系数，范围 [0.0, 1.0]
        _tick_timeout (float): 单次 tick 超时时间（单位：秒）
        _stop_timeout (float): 停止等待超时时间（单位：秒）
        _task (asyncio.Task | None): 后台任务引用（运行时创建）
        _stopped (asyncio.Event): 停止信号（Event 对象）
        _limiter (ResourceLimiter | None): 资源限流器
        _event_bus (EventBus | None): 事件总线

    Methods:
        start: 启动后台任务（幂等）
        stop: 优雅停止后台任务
        update_intervals: 动态更新轮询间隔（热重载）
        tick: 业务逻辑（抽象方法，由子类实现）
        _run_loop: 任务调度循环（私有方法，固化在基类）

    Note:
        - 该类是抽象基类（ABC），不能直接实例化
        - 子类必须实现 tick() 方法
        - start() 和 stop() 是幂等的（可重复调用）
        - 不是线程安全的（仅支持 asyncio 环境）
    """

    def __init__(
            self,
            *,
            name: str,
            logger: Any,
            min_interval: float = 2.0,
            max_interval: float = 30.0,
            jitter: float = 0.1,
            tick_timeout: float = DEFAULT_TICK_TIMEOUT,
            stop_timeout: float = STOP_TIMEOUT,
            limiter: ResourceLimiter | None = None,
            event_bus: EventBus | None = None,
    ) -> None:
        """初始化后台观察者

        Args:
            name (str): 观察者名称，用于日志和任务标识
            logger (Any): 日志记录器
            min_interval (float, optional): 最小轮询间隔（单位：秒）。默认 2.0
            max_interval (float, optional): 最大轮询间隔（单位：秒）。默认 30.0
            jitter (float, optional): 抖动系数，范围 [0.0, 1.0]。默认 0.1（10% 抖动）
            tick_timeout (float, optional): 单次 tick 超时时间（单位：秒）。默认 DEFAULT_TICK_TIMEOUT（60 秒）
            stop_timeout (float, optional): 停止等待超时时间（单位：秒）。默认 STOP_TIMEOUT（5 秒）
            limiter (ResourceLimiter | None, optional): 资源限流器，用于控制 tick 执行频率
            event_bus (EventBus | None, optional): 事件总线，用于发布内部事件（如限流告警）

        Note:
            参数验证和规范化：
            - jitter: 强制限制在 [0.0, 1.0] 范围内
            - tick_timeout: 最小值 0.1 秒（避免过于激进的超时）
            - stop_timeout: 最小值 0.1 秒（避免无法正常退出）
            - min_interval 和 max_interval: 由 update_intervals() 验证
        """
        # 基本属性
        self._name = name
        self._logger = logger

        # 抖动系数规范化：限制在 [0.0, 1.0] 范围内
        self._jitter = max(0.0, min(jitter, 1.0))

        # tick 超时规范化：最小 0.1 秒
        self._tick_timeout = max(0.1, tick_timeout)

        # 停止超时规范化：最小 0.1 秒
        self._stop_timeout = max(0.1, stop_timeout)

        # 保存限流器和事件总线
        self._limiter = limiter
        self._event_bus = event_bus

        # 自动装配：如果同时提供了 limiter 和 event_bus，自动进行连接
        # 这确保了 limiter 能够利用 event_bus 发送限流告警
        if self._limiter:
            if self._event_bus:
                self._limiter.event_bus = self._event_bus

            # 自动设置限流器名称，便于在告警事件中识别来源（如 "apollo-watcher-limiter"）
            self._limiter.name = f"{self._name}-limiter"

        # 显式定义实例属性，这些属性会在 update_intervals() 中被初始化
        self._min_interval: float = 2.0
        self._max_interval: float = 30.0

        # 初始化轮询间隔（含验证和规范化）
        self.update_intervals(min_interval, max_interval)

        # 任务引用（运行时创建），None: 任务未启动，asyncio.Task: 任务正在运行
        self._task: asyncio.Task[None] | None = None

        # 停止信号（asyncio.Event 对象）
        self._stopped = asyncio.Event()

    def update_intervals(self, min_interval: float, max_interval: float) -> None:
        """动态更新轮询间隔（支持热重载）"""
        # 验证和规范化 min_interval：最小值 0.1 秒
        self._min_interval = max(0.1, min_interval)
        # 验证和规范化 max_interval：不能小于 min_interval
        self._max_interval = max(self._min_interval, max_interval)

    async def start(self) -> None:
        """启动后台任务（幂等）"""
        if self._task is not None:
            return

        self._stopped.clear()
        self._task = asyncio.create_task(
            self._run_loop(), name=f"watcher:{self._name}"
        )
        await asyncio.sleep(0)
        self._logger.info(f"后台观察者[{self._name}] 已启动。")

    async def stop(self) -> None:
        """优雅停止后台任务"""
        if self._task is None:
            return

        self._stopped.set()
        self._task.cancel()

        try:
            await asyncio.wait_for(self._task, timeout=self._stop_timeout)
        except asyncio.TimeoutError:
            self._logger.warning(
                f"后台观察者[{self._name}] 在 {self._stop_timeout}秒内未能停止，强制放弃引用。"
            )
        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            if current_task and current_task.cancelled():
                raise
        except Exception as e:
            self._logger.error(f"后台观察者[{self._name}] 停止时发生异常: {e}")
        finally:
            self._task = None

        self._logger.info(f"后台观察者[{self._name}] 已停止。")

    async def _run_loop(self) -> None:
        """任务调度循环（私有方法，固化在基类）

        该方法实现了核心的任务调度逻辑：
        1. 执行 tick()（业务逻辑），若配置了限流器，自动应用限流
        2. 异常处理和退避重试
        3. 可中断睡眠（响应停止信号）
        """
        # 初始化轮询间隔为最小值
        interval = self._min_interval

        # 主循环：持续执行，直到收到停止信号
        while not self._stopped.is_set():
            # 记录本次 tick 的开始时间
            started = time.perf_counter()

            try:
                # 执行业务逻辑（含限流和超时保护）
                if self._limiter:
                    # 若配置了限流器，使用 limiter.run() 包装 tick()
                    # 这会自动处理并发控制和 QPS 限制，并在排队过久时触发 EventBus 告警
                    # 注意：超时时间包含"等待限流槽位" + "执行 tick" 的总时间
                    # 这是一种保护机制：如果因限流排队太久导致超时，说明系统过载，应放弃本次 tick
                    await asyncio.wait_for(
                        self._limiter.run(self.tick),
                        timeout=self._tick_timeout
                    )
                else:
                    # 无限流器，直接执行
                    await asyncio.wait_for(self.tick(), timeout=self._tick_timeout)

                # tick() 成功执行，重置退避间隔为最小值
                interval = self._min_interval

            except asyncio.CancelledError:
                self._logger.debug(f"后台观察者[{self._name}] 任务被取消。")
                raise

            except asyncio.TimeoutError:
                # tick() 执行超时（或限流排队超时）
                self._logger.error(
                    f"后台观察者[{self._name}] 执行超时 (> {self._tick_timeout}秒)。"
                )
                interval = min(interval * 2, self._max_interval)

            except Exception:
                # tick() 抛出业务异常
                self._logger.exception(f"后台观察者[{self._name}] 执行出错")
                interval = min(interval * 2, self._max_interval)

            # 计算休眠时间（含抖动）
            elapsed = time.perf_counter() - started
            sleep_for = max(interval - elapsed, 0.1)

            if self._jitter > 0:
                sleep_for *= random.uniform(1 - self._jitter, 1 + self._jitter)

            try:
                # 可中断睡眠
                await asyncio.wait_for(self._stopped.wait(), timeout=sleep_for)
                break
            except asyncio.TimeoutError:
                continue

    @abstractmethod
    async def tick(self) -> None:
        """业务逻辑（抽象方法，由子类实现）

        该方法定义了每次轮询时要执行的具体业务逻辑。子类必须实现该方法，否则无法实例化

        Raises:
            NotImplementedError: 若子类未实现该方法

        Note:
            实现要求：
            - 必须是异步方法（async def）
            - 应该是幂等的（可重复执行）
            - 应该处理异常（避免崩溃）
            - 应该支持取消（响应 CancelledError）
            - 执行时间应该小于 tick_timeout（避免超时）
        """
        raise NotImplementedError
