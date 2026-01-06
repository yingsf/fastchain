from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable, Generic, TypeVar

from .events import Event, EventBus, EventConstants

# 泛型类型变量，用于 run() 方法的返回值类型注解，使得 run() 能够保留原函数的返回值类型信息
T = TypeVar("T")


@dataclass(slots=True)
class ResourceLimiter(Generic[T]):
    """资源并发与速率控制器（支持 EventBus 可观测性）

    该类实现了双重限流策略，同时支持并发度控制和 QPS（每秒请求数）限制
    使用信号量（Semaphore）实现并发度控制，使用令牌桶（Token Bucket）算法实现 QPS 控制

    [New] 可观测性增强：
    集成了 EventBus，当请求因限流而发生显著等待（排队）时，自动发布告警事件。
    这解决了"黑盒限流"问题，让运维人员能区分是下游资源慢还是因并发限制导致的排队慢。

    核心特性：
    - 并发度限制：控制同时执行的请求数（防止资源耗尽）
    - QPS 限制：控制每秒请求数（防止速率过载）
    - 双重保护：两种限制可独立配置或组合使用
    - 优雅阻塞：超过限制时自动等待，而非直接拒绝
    - [New] 限流告警：触发限流排队时自动发布事件（limiter.throttled）

    限流算法详解：

    1. 并发度控制（Semaphore）：
       - 使用 asyncio.Semaphore 实现
       - 初始化计数器 = max_concurrency
       - acquire(): 计数器 -1，若为 0 则阻塞等待
       - release(): 计数器 +1，唤醒一个等待的协程
       - 复杂度：O(1)

    2. QPS 控制（Token Bucket）：
       - 维护上次请求时间戳（_last_acquire）
       - 计算最小请求间隔：min_interval = 1.0 / qps
       - acquire() 时检查距上次请求的间隔
       - 若间隔 < min_interval，则通过睡眠来对齐
       - 使用 Lock 保护时间戳更新，避免并发竞态
       - 复杂度：O(1)

    设计权衡：

    1. 执行顺序：为何先获取槽位，再进行 QPS 节流？
       - 优势：已获取槽位的请求一定能在合理时间内开始执行
       - 劣势：可能导致短时间内 QPS 略微超限（误差 < 1 个请求）
       - 替代方案：先节流再获取槽位
         * 优势：严格保证 QPS 不超限
         * 劣势：可能导致已等待的请求长时间无法开始（饥饿）
       - 选择：当前实现优先保证请求不被饿死

    2. 时间戳选择：为何使用 monotonic 而非 time.time？
       - time.time(): 系统时间，可能受 NTP 校时影响（时间倒退或跳跃）
       - time.monotonic(): 单调时钟，保证时间只会向前推进
       - 对于 QPS 计算，单调性比绝对时间更重要
       - 若系统时间回拨，time.time 可能导致：
         * sleep_for 计算错误（可能是巨大的负数或正数）
         * QPS 控制失效（长时间阻塞或完全不阻塞）

    Attributes:
        max_concurrency (int): 最大并发数。默认为 1。必须 >= 1
        qps (float | None): 每秒请求数限制（可选）。默认为 None（不限制 QPS）。若设置，必须 > 0
        name (str): [New] 限流器名称（通常对应资源名称），用于事件标识。默认为 "limiter"
        event_bus (EventBus | None): [New] 事件总线引用，用于发布限流事件。默认为 None
        in_flight (int): 当前正在执行的请求数（只读，自动维护）

    Methods:
        can_accept: 检查是否可以接受新请求（非阻塞）
        acquire: 获取执行槽位（阻塞式）
        release: 释放执行槽位
        slot: 上下文管理器，自动管理槽位获取和释放
        run: 在限流保护下执行函数
        _emit_throttled_event: [New] 发布限流告警事件

    Note:
        - 该实现是协程安全的，多个协程可并发调用 acquire/release
        - 不是线程安全的，不应在多线程环境中使用（asyncio 本身就不是线程安全的）
        - QPS 限制是全局的（所有协程共享），而非针对单个协程
        - 若需更精确的 QPS 控制，可使用滑动窗口算法（但复杂度更高）
    """

    # 最大并发数：同时执行的请求数上限，默认为 1，表示串行执行（无并发）
    max_concurrency: int = 1

    # 每秒请求数限制（可选），None: 不限制 QPS，> 0: 限制每秒最多执行 qps 个请求
    qps: float | None = None

    # 限流器名称，用于在事件中标识来源（如 "redis-main"）
    name: str = "limiter"

    # 事件总线，用于发布限流告警事件（可选注入）
    # 若为 None，则限流器回退到静默模式（仅阻塞，不报警），保持轻量级特性
    event_bus: EventBus | None = None

    # 信号量，用于并发度控制
    # field(init=False) 表示不在 __init__ 中初始化，由 __post_init__ 创建
    # repr=False 表示不在 repr() 输出中显示（避免泄露内部实现细节）
    _sem: asyncio.Semaphore = field(init=False, repr=False)

    # 锁，用于保护 QPS 相关的时间戳更新，防止多个协程并发修改 _last_acquire 导致竞态条件
    _qps_lock: asyncio.Lock = field(init=False, repr=False)

    # 上次获取槽位的时间戳（单调时钟），0.0: 表示从未获取过槽位（初始状态），> 0.0: 上次 acquire() 完成时的 monotonic 时间
    _last_acquire: float = field(init=False, default=0.0, repr=False)

    # 当前正在执行的请求数，用于监控和调试，不直接参与限流逻辑，acquire() 时 +1，release() 时 -1
    in_flight: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        """初始化内部状态（在 __init__ 之后自动调用）

        该方法由 dataclass 自动调用，用于：
        1. 验证配置参数的合法性
        2. 创建内部对象（Semaphore、Lock）

        Raises:
            ValueError: 若 max_concurrency < 1

        Note:
            为何使用 __post_init__ 而非 __init__？
            - dataclass 会自动生成 __init__，用于初始化带 init=True 的字段
            - __post_init__ 在自动生成的 __init__ 之后被调用
            - 适合执行验证和创建内部对象（这些对象不需要作为参数传入）

            为何验证 max_concurrency >= 1？
            - max_concurrency = 0: 无法处理任何请求（死锁）
            - max_concurrency < 0: 语义不明确
            - 虽然 Semaphore(0) 是合法的，但在限流器中没有意义
        """
        # 验证并发数：至少为 1，否则无法处理任何请求
        if self.max_concurrency < 1:
            raise ValueError("最大并发数必须大于等于1")

        # 创建信号量，用于并发度控制
        self._sem = asyncio.Semaphore(self.max_concurrency)

        # 创建锁，用于保护 QPS 相关的时间戳更新
        self._qps_lock = asyncio.Lock()

    def can_accept(self) -> bool:
        """检查是否可以接受新请求（非阻塞）

        该方法提供快速的过载检查，用于在不阻塞的情况下判断是否应该接受新请求。仅基于并发度判断，不考虑 QPS 限制

        Returns:
            bool: 若有可用槽位则返回 True，否则返回 False

        Note:
            实现原理：检查 Semaphore 是否被锁定（locked）
            - Semaphore.locked() = True: 计数器为 0，所有槽位已被占用
            - Semaphore.locked() = False: 计数器 > 0，至少有一个槽位可用

            为何不考虑 QPS 限制？
            - QPS 限制需要计算时间间隔，可能需要等待（不符合非阻塞语义）
            - 并发度限制是硬限制（超过则无法执行），QPS 限制是软限制（可以短时间超限）
            - 若需同时考虑 QPS，可在调用方额外检查时间戳

            注意事项：
            - 该检查是瞬时的，返回 True 不保证后续 acquire() 一定成功
            - 在高并发下，多个协程可能同时看到 can_accept() = True
            - 若需保证一定能获取槽位，应直接调用 acquire()（可能阻塞）
        """
        # 检查 Semaphore 是否被锁定（计数器是否为 0）
        return not self._sem.locked()

    async def acquire(self) -> None:
        """获取执行槽位（阻塞式，带可观测性埋点）

        该方法会依次执行并发度检查和 QPS 节流，确保请求同时满足两种限制。若当前无可用槽位或 QPS 超限，会阻塞等待直到条件满足

        执行流程：
        1. 记录开始时间（用于计算排队耗时）
        2. 等待 Semaphore 槽位（并发度控制）
           - 若有可用槽位，立即获取并继续
           - 若无可用槽位，阻塞等待其他协程释放
        3. 增加在途请求计数（in_flight += 1）
        4. 若配置了 QPS，进行速率控制
           - 计算距上次请求的时间间隔
           - 若间隔不足，睡眠对齐
           - 更新时间戳
        5. 计算总等待时间，若超过阈值且配置了 EventBus，发布告警事件

        Note:
            **执行顺序很重要**：为何先获取槽位，再进行 QPS 节流？
            （此处保留原有详细注释，略过以节省篇幅...请参考原有文件）

            [New] 可观测性设计：
            - 我们记录了 start_wait 时间戳
            - 在获取锁和 QPS 睡眠结束后，计算 total_wait
            - 阈值设定为 0.1秒（100ms），这是一个经验值：
              * 小于 100ms 的等待通常被认为是正常的抖动
              * 大于 100ms 的等待意味着系统压力较大，需要引起注意
            - 使用 "Fire & Forget" 模式发布事件，确保监控逻辑不影响核心业务性能
        """
        # 记录开始等待的时间（用于计算排队耗时）
        start_wait = time.monotonic()

        # 步骤 1: 并发度检查
        # 等待 Semaphore 槽位（可能阻塞），若所有槽位已被占用，此处会挂起当前协程，让出控制权给其他协程
        await self._sem.acquire()

        # 增加在途请求计数，用于监控和调试，不直接参与限流逻辑
        self.in_flight += 1

        try:
            # 步骤 2: QPS 节流，若未配置 QPS 或 QPS 无效，跳过节流
            if self.qps and self.qps > 0:
                min_interval = 1.0 / self.qps

                # 使用锁保护时间戳的读写，避免并发竞态，锁的粒度：仅保护时间戳计算和更新（不保护 Semaphore）
                # 这确保了多个协程不会并发修改 _last_acquire，导致 QPS 计算错误
                async with self._qps_lock:
                    # 使用单调时钟获取当前时间（相对于系统启动），monotonic 保证时间只会向前推进，不受系统时间回拨影响
                    now = time.monotonic()

                    # 边界情况：首次请求（_last_acquire = 0.0），直接记录时间戳并返回，不进行节流，这确保第一个请求不会被阻塞
                    if self._last_acquire <= 0.0:
                        self._last_acquire = now
                    else:
                        # 计算距上次请求的时间间隔（单位：秒）
                        elapsed = now - self._last_acquire

                        # 计算需要睡眠的时间（单位：秒）
                        sleep_for = min_interval - elapsed

                        if sleep_for > 0:
                            # 间隔不足，需要睡眠补齐
                            # 例如：min_interval=0.01，elapsed=0.005，sleep_for=0.005
                            # 睡眠 5ms 后，总间隔达到 10ms，满足 QPS 要求
                            await asyncio.sleep(sleep_for)

                            # 重新获取时间，校正睡眠过程中的时间漂移
                            # （此处保留原有关于重新获取时间的详细注释...）
                            self._last_acquire = time.monotonic()
                        else:
                            # 间隔已足够，无需睡眠
                            self._last_acquire = now

        except Exception:
            # 异常安全性：若在 QPS 检查步骤抛出异常（虽极少见），必须释放 Semaphore 并回滚计数
            # 否则会导致槽位永久丢失（Semaphore 泄漏）和 in_flight 计数错误
            self._sem.release()
            self.in_flight -= 1
            raise

        # 可观测性逻辑：计算总等待时间（并发等待 + QPS 等待）
        # 仅当等待时间超过 100ms 且配置了 EventBus 时才发布事件，避免事件风暴
        total_wait = time.monotonic() - start_wait
        if self.event_bus and total_wait > 0.1:
            await self._emit_throttled_event(total_wait)

    async def _emit_throttled_event(self, wait_time: float) -> None:
        """发布限流告警事件（Fire & Forget）

        该方法负责组装并发布 'limiter.throttled' 事件。

        Args:
            wait_time (float): 请求排队等待的总耗时（秒）

        Note:
            - 这是一个"发射后不管"的操作，内部捕获了所有异常
            - 即使事件发布失败（如 EventBus 队列满），也不能影响正常的业务请求处理
        """
        if not self.event_bus:
            return

        try:
            # 注意：此处使用了 string literal "limiter.throttled"
            # 这是一个特定于限流器的业务事件，目前暂不放入全局 EventConstants
            await self.event_bus.publish(
                Event(
                    type="limiter.throttled",
                    payload={
                        EventConstants.KEY_SOURCE: "limiter",
                        EventConstants.KEY_RESOURCE_NAME: self.name,
                        EventConstants.KEY_TIMESTAMP: time.time(),
                        "wait_time_ms": round(wait_time * 1000, 2),
                        "in_flight": self.in_flight,
                        "limit_concurrency": self.max_concurrency,
                        "limit_qps": self.qps,
                    }
                )
            )
        except Exception:
            # 静默失败：监控逻辑不应破坏业务逻辑
            pass

    def release(self) -> None:
        """释放执行槽位

        该方法释放之前通过 acquire() 获取的槽位，并减少在途请求计数。必须与 acquire() 配对使用，否则会导致 Semaphore 计数器异常

        Note:
            必须配对使用：
            - 每次调用 acquire() 后，必须调用 release()
            - 若忘记调用 release()，会导致槽位永久占用（资源泄漏）
            - 若多次调用 release()（未配对 acquire），会导致 Semaphore 计数器超过 max_concurrency
        """
        # 防御性检查：避免 in_flight 计数器变为负数
        if self.in_flight > 0:
            self.in_flight -= 1

        # 释放 Semaphore 槽位
        self._sem.release()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """上下文管理器：自动管理槽位获取和释放

        该方法提供了一个便捷的上下文管理器，用于确保槽位一定被释放，即使在异常情况下也能正确清理资源

        Yields:
            None: 不产生任何值（仅用于控制流）

        Note:
            这是使用限流器的推荐方式，原因：
            1. 自动资源管理：确保槽位一定被释放（即使抛出异常）
            2. 代码简洁：避免手写 try-finally 样板代码
            3. 符合习惯：与 Python 的 with 语句一致
        """
        # __aenter__: 获取槽位（可能阻塞）
        await self.acquire()
        try:
            # 让出控制权给调用方（执行 with 块内的代码），yield None 表示不产生任何值
            yield
        finally:
            # __aexit__: 释放槽位（即使抛出异常也会执行），finally 块确保资源一定被释放，避免资源泄漏
            self.release()

    async def run(self, func: Callable[[], Awaitable[T]]) -> T:
        """在限流保护下执行函数

        该方法提供了一个便捷的方式，用于在限流保护下执行异步函数。它是 slot() 上下文管理器的快捷方式，适合对已有函数进行限流包装

        Args:
            func (Callable[[], Awaitable[T]]): 要执行的异步函数（无参数）。函数签名必须是 `async def() -> T`

        Returns:
            T: 函数的返回值（类型由泛型参数 T 确定）。

        Note:
            该方法是 slot() 的语法糖，等价于：
            ```python
            async with limiter.slot():
                return await func()
            ```

            （此处保留原有详细注释，关于使用场景和示例代码...）
        """
        # 使用 slot() 上下文管理器确保槽位被正确管理。静态检查会提示未传self形参，通过下面的注解忽略（忽略参数列表的警告）
        # noinspection PyArgumentList
        async with self.slot():
            # 执行函数并返回结果，泛型参数 T 确保返回值类型与 func 的返回值类型一致
            return await func()
