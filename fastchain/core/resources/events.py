from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    List,
    Protocol,
    runtime_checkable,
)

from ...core.config.constants import AppConstants


SHUTDOWN_EVENT = "_eventbus_stop"


class EventConstants:
    """事件系统常量注册表

    集中管理所有的事件类型（Topic）和载荷键（Payload Key），防止硬编码导致的拼写错误，并明确跨模块通信的契约
    """
    # --- 事件类型 (Event Types) ---

    # 资源生命周期事件
    RESOURCE_STARTED = "resource.started"
    RESOURCE_STOPPED = "resource.stopped"
    RESOURCE_START_FAILED = "resource.start_failed"

    # 配置变更事件
    # 当 Apollo 或其他配置源更新时触发
    CONFIG_UPDATED = "config.updated"

    # LLM 审计事件
    # 用于解耦 LLM 调用层与审计日志持久化层
    LLM_AUDIT_SUCCESS = "llm.audit.success"
    LLM_AUDIT_ERROR = "llm.audit.error"

    # 任务调度事件
    JOB_STARTED = "job.started"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"

    # 内部信号
    _SHUTDOWN = SHUTDOWN_EVENT

    # --- 载荷键名 (Payload Keys) ---
    # 标准化 Payload 中的 Key，方便消费者解析

    # 1. 通用基础字段
    # 事件来源，如 'apollo', 'env'
    KEY_SOURCE = "source"
    # 时间戳
    KEY_TIMESTAMP = "timestamp"
    # 全链路追踪 ID
    KEY_TRACE_ID = "trace_id"
    # 变更的命名空间列表
    KEY_UPDATED_NAMESPACES = "updated_namespaces"
    # 资源名称
    KEY_RESOURCE_NAME = "name"
    # 资源类型
    KEY_RESOURCE_KIND = "kind"
    # 错误信息 (原 error_message)
    KEY_ERROR = "error"
    # 状态 (success/error)
    KEY_STATUS = "status"

    # 2. LLM 审计专用字段
    # 模型别名
    KEY_MODEL_ALIAS = "model_alias"
    # 业务场景
    KEY_SCENE = "scene"
    # 总耗时(ms)
    KEY_LATENCY_MS = "latency_ms"
    # 首字延迟(ms)
    KEY_TTFT_MS = "ttft_ms"
    # 输入数据
    KEY_INPUT = "input"
    # 输出数据
    KEY_OUTPUT = "output"
    # 元数据
    KEY_METADATA = "metadata"
    # 错误类型名
    KEY_ERROR_TYPE = "error_type"

    # 3. Token 统计字段
    KEY_PROMPT_TOKENS = "prompt_tokens"
    KEY_COMPLETION_TOKENS = "completion_tokens"
    KEY_TOTAL_TOKENS = "total_tokens"

    # 4. 任务调度专用字段
    KEY_JOB_ID = "job_id"
    KEY_DURATION_MS = "duration_ms"


@dataclass(slots=True)
class Event:
    """事件数据类

    该数据类封装了事件总线中流转的事件对象，包含事件类型、时间戳和载荷。使用 dataclass 简化定义，使用 slots=True 优化内存占用

    Attributes:
        type (str): 事件类型标识符（如 "resource.started"、"user.login"）
        timestamp (float): 事件发生的 Unix 时间戳（单位：秒）。默认为当前时间
        payload (Dict[str, Any]): 事件载荷，包含事件相关的上下文信息（如资源元数据、用户 ID）。默认为空字典

    Note:
        - 使用 slots=True 优化内存占用，适合大量创建事件对象的场景（如每秒数千事件）
        - timestamp 使用 time.time() 而非 time.monotonic()，是因为：
          * 事件时间戳需要与外部系统（如日志、监控、数据库）对齐
          * monotonic 时间是相对时间，不适合跨进程/跨系统传递
          * 虽然 time.time() 可能受系统时间调整影响（如 NTP 同步），但对于事件记录来说可接受
        - payload 使用可变字典，调用方应避免在事件发布后修改载荷（建议使用不可变数据）
    """
    type: str
    timestamp: float = field(default_factory=time.time)
    # 使用 dict 而非更严格的类型（如 Mapping），是为了方便调用方构造和修改载荷
    # 但调用方应遵循最佳实践：在事件发布后不修改载荷（视为不可变）
    payload: Dict[str, Any] = field(default_factory=dict)


# 事件处理器类型别名：接收 Event，返回 None 的异步函数
EventHandler = Callable[[Event], Awaitable[None]]


@runtime_checkable
class LoggerLike(Protocol):
    """日志记录器协议，定义日志记录器的最小接口

    该协议使用 typing.Protocol 定义鸭子类型，兼容标准库 logging 和第三方库 loguru
    通过协议而非具体类型，允许调用方传入任何实现了这些方法的日志对象

    Methods:
        debug: 记录调试级别日志
        info: 记录信息级别日志
        warning: 记录警告级别日志
        error: 记录错误级别日志

    Note:
        - 使用 Protocol 而非 ABC（抽象基类），遵循"鸭子类型"原则，提高代码灵活性
        - @runtime_checkable 允许在运行时使用 isinstance() 检查对象是否满足协议
        - 方法签名使用 __msg（双下划线前缀）表示位置参数，与 logging 模块的签名一致
        - 该协议不要求实现 critical() 等其他日志方法，只定义事件总线实际使用的方法
    """

    def debug(self, __msg: str, *args: Any, **kwargs: Any) -> None:
        """记录调试级别日志

        Args:
            __msg (str): 日志消息模板（支持 % 格式化）
            *args: 位置参数，用于格式化消息
            **kwargs: 关键字参数（如 exc_info、stack_info）
        """
        ...

    def info(self, __msg: str, *args: Any, **kwargs: Any) -> None:
        """记录信息级别日志

        Args:
            __msg (str): 日志消息模板
            *args: 位置参数
            **kwargs: 关键字参数
        """
        ...

    def warning(self, __msg: str, *args: Any, **kwargs: Any) -> None:
        """记录警告级别日志

        Args:
            __msg (str): 日志消息模板
            *args: 位置参数
            **kwargs: 关键字参数
        """
        ...

    def error(self, __msg: str, *args: Any, **kwargs: Any) -> None:
        """记录错误级别日志

        Args:
            __msg (str): 日志消息模板
            *args: 位置参数
            **kwargs: 关键字参数
        """
        ...


class EventBus:
    """轻量级进程内异步事件总线

    该类实现了基于 asyncio 的发布/订阅模式事件总线，适用于进程内的松耦合组件通信
    通过异步队列实现生产者-消费者模式，支持背压保护、优雅关闭和异常隔离

    核心特性：
    - 异步非阻塞：发布事件不阻塞调用方（除非队列满时执行 Ring Buffer）
    - 背压保护：队列满时自动丢弃最旧事件，防止内存溢出
    - 优雅关闭：通过哨兵事件确保剩余事件被处理完毕
    - 异常隔离：单个处理器失败不影响其他处理器
    - 多订阅者：同一事件类型可有多个处理器，按注册顺序执行

    架构设计：
    ```
    生产者（调用方）              事件总线                     消费者（处理器）
         |                          |                              |
         | publish(event) --------> | asyncio.Queue -----------> | _worker()
         |   (非阻塞)               |   (缓冲 1000 事件)         |    |
         |                          |                            |    v
         |                          |                            | handler1(event)
         |                          |                            | handler2(event)
         |                          |                            | ...
    ```

    背压保护策略（Ring Buffer）：
    - 队列大小限制为 1000 事件（可配置）
    - 队列满时，执行 "弹出最旧 + 插入最新" 的原子操作
    - 使用锁保护该操作，防止并发 publish 导致竞态条件
    - 适用于可容忍部分事件丢失的场景（如监控指标、审计日志）

    并发安全性：
    - subscribe() 不是线程安全的，应在应用启动阶段调用（单线程）
    - publish() 是协程安全的，多个协程可并发调用
    - Ring Buffer 使用锁保护，确保 "弹出 + 插入" 的原子性

    Attributes:
        self._handlers (DefaultDict[str, List[EventHandler]]): 事件处理器字典，键为事件类型
        self._queue (asyncio.Queue[Event]): 异步队列，用于缓冲待处理的事件
        self._is_running (bool): 运行状态标志
        self._worker_task (asyncio.Task[None] | None): 后台工作任务句柄
        self._logger (LoggerLike): 日志记录器
        self._publish_lock (asyncio.Lock): 发布锁，保护 Ring Buffer 逻辑

    Methods:
        subscribe: 订阅事件类型
        publish: 发布事件（非阻塞，Best-effort）
        start: 启动事件总线
        stop: 优雅停止事件总线

    Note:
        - 该实现是进程内的，不支持跨进程/跨机器通信（需使用 Redis Pub/Sub、RabbitMQ 等）
        - 事件处理是异步的，publish() 返回时事件可能尚未被处理
        - 若需要同步等待事件处理完成，可使用 asyncio.Event 或 Future
    """

    def __init__(self, logger: LoggerLike | None = None) -> None:
        """初始化事件总线

        Args:
            logger (LoggerLike | None):
              日志记录器（可选）。默认为 None，会使用标准库 logging。支持任何实现了 LoggerLike 协议的对象（如 loguru.logger）

        Note:
            队列大小设置为 1000，是基于以下考虑：
            - 太小（如 100）：容易触发 Ring Buffer，导致事件丢失率过高
            - 太大（如 10000）：占用过多内存，且异常情况下积压过多事件
            - 1000 是一个经验值，适合大多数场景（每秒数百事件）

            若事件产生速率极高（如每秒数千事件），应考虑：
            1. 异步化事件处理器（避免阻塞 worker）
            2. 引入外部消息队列（如 Redis Streams、Kafka）
            3. 增大队列大小（但需权衡内存占用）
            4. 采样策略（如仅记录 10% 的事件）
        """
        # 事件处理器字典：每个事件类型可以有多个处理器，使用 defaultdict(list) 避免手动检查键是否存在
        self._handlers: DefaultDict[str, List[EventHandler]] = defaultdict(list)

        # 异步队列：缓冲待处理的事件，maxsize=1000 提供背压保护，队列满时，publish() 会触发 Ring Buffer 策略（丢弃最旧事件）
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=1000)

        # 运行状态：用于控制 publish() 和 _worker() 的行为，False: 拒绝新事件，worker 停止处理，True: 接受新事件，worker 持续处理
        self._is_running: bool = False

        # 后台工作任务句柄：用于优雅关闭时等待任务完成，None: 任务未创建或已完成，Task: 任务正在运行
        self._worker_task: asyncio.Task[None] | None = None

        # 日志记录器：若未提供则使用标准库 logging，使用固定的日志名称便于配置和过滤
        self._logger: LoggerLike = logger or logging.getLogger(
            f"{AppConstants.DEFAULT_APP_NAME}.core.events"
        )

        # 发布锁：保护 Ring Buffer 逻辑的并发安全性，防止多个协程同时执行 "弹出旧事件 + 插入新事件" 导致竞态条件
        self._publish_lock = asyncio.Lock()

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """订阅事件类型，注册事件处理器

        该方法将事件处理器注册到指定的事件类型，当该类型的事件被发布时，处理器会被异步调用
        同一事件类型可以注册多个处理器，它们会按注册顺序依次执行

        Args:
            event_type (str): 事件类型标识符（如 "resource.started"）。建议使用命名空间前缀，避免不同模块间的事件类型冲突
            handler (EventHandler): 事件处理器函数（异步函数，签名为 async def(event: Event) -> None）

        Note:
            处理器设计建议：
            - 轻量级：处理器应快速返回（< 100ms），避免阻塞事件循环
            - 异步化耗时操作：若需执行耗时操作（如数据库写入、HTTP 请求），应在处理器内启动新任务
            - 异常处理：处理器应捕获预期的异常，未捕获的异常会被总线捕获并记录日志
            - 幂等性：处理器应是幂等的，因为在某些异常情况下事件可能被重复处理

            线程安全性：
            当前实现不是线程安全的（在 asyncio 事件循环中（单线程）是绝对安全的），所以应在单线程环境中调用（通常在应用启动阶段）
            若需要在运行时动态订阅（多线程/多协程），应考虑：
            1. 加锁保护 _handlers 字典
            2. 使用线程安全的数据结构（如 collections.deque）
            3. 使用写时复制（Copy-on-Write）策略
        """
        # 将处理器追加到该事件类型的处理器列表
        # defaultdict 确保即使 event_type 不存在也会自动创建空列表
        # 处理器会按注册顺序依次执行（FIFO）
        self._handlers[event_type].append(handler)

    async def publish(self, event: Event) -> None:
        """发布事件（非阻塞，Best-effort 语义）

        该方法将事件发布到事件总线，由后台 worker 异步处理。发布操作是非阻塞的，即方法返回时事件可能尚未被处理（仅保证已加入队列或被丢弃）

        Args:
            event (Event): 要发布的事件对象

        Note:
            非阻塞语义：
            - 快速路径：若队列未满，立即返回（事件已加入队列，时间复杂度 O(1)）
            - 慢速路径：若队列已满，执行 Ring Buffer 策略（丢弃最旧事件，时间复杂度 O(1)）
            - 最坏情况：若 Ring Buffer 也失败（极端并发），丢弃当前事件并记录警告

            Ring Buffer 策略的必要性：
            在高负载下，若事件产生速率 > 处理速率，队列会持续增长导致内存溢出
            Ring Buffer 策略通过丢弃旧事件保证内存可控，适用于可容忍部分事件丢失的场景

            适用场景：
            - ✅ 监控指标（丢失少量数据点可接受）
            - ✅ 审计日志（非关键事件可丢失）
            - ✅ 用户行为追踪（采样即可）
            - ❌ 关键业务流程（需要可靠交付保证）

            并发安全性分析：
            1. 问题：多个协程并发 publish，都检测到队列满
            2. 风险：竞态条件导致队列状态不一致（超过 maxsize 或丢失多个事件）
            3. 解决方案：使用锁保护 "弹出旧事件 + 插入新事件" 的原子性
            4. 优化：在锁内再次尝试 put_nowait（可能在等待锁时有空间了）
        """
        # 检查总线是否已启动
        if not self._is_running:
            # 总线未启动，丢弃事件并记录调试日志
            self._logger.debug("事件总线未运行，丢弃事件: %s", event.type)
            return

        # 让出控制权，确保不阻塞当前协程，这对于在高负载下保持系统响应性很重要，避免单个协程长时间占用事件循环
        # sleep(0) 会立即返回，但会触发事件循环调度其他就绪的协程
        await asyncio.sleep(0)

        # 快速路径：尝试直接放入队列
        try:
            # 非阻塞插入：若队列未满，立即返回（时间复杂度 O(1)）
            self._queue.put_nowait(event)
            return
        except asyncio.QueueFull:
            # 队列已满，需要执行 Ring Buffer 策略，继续执行慢速路径（不在这里直接处理，避免代码重复）
            pass

        # 慢速路径：Ring Buffer 策略
        # 队列已满，需要执行 "弹出最旧的 + 插入新的" 的原子操作，使用锁确保原子性，防止并发 publish 导致竞态条件
        async with self._publish_lock:
            # 双重检查：在等待锁的时候，可能其他协程已经处理了队列，再次尝试直接插入，避免不必要的 Ring Buffer 操作
            try:
                self._queue.put_nowait(event)
                return
            except asyncio.QueueFull:
                # 确实满了，执行 Ring Buffer 策略
                pass

            # Ring Buffer 核心逻辑：弹出最旧的 + 插入新的
            try:
                # 步骤 1: 弹出最旧的事件（FIFO 队列，队头是最旧的），get_nowait() 不会阻塞，若队列为空则抛出 QueueEmpty
                _ = self._queue.get_nowait()

                # 步骤 2: 插入新事件，此时队列应该有一个空位（刚刚弹出了一个）
                self._queue.put_nowait(event)

            except (asyncio.QueueEmpty, asyncio.QueueFull):
                # 极端的边界情况（几乎不可能发生，但需要防御性处理）：
                #
                # QueueEmpty: 在 get_nowait 和 put_nowait 之间，其他协程清空了队列
                #   - 原因：虽然有锁保护，但锁只保护当前 publish 调用
                #   - 可能性：极低（需要 worker 在极短时间内处理完所有事件）
                #
                # QueueFull: 在 get_nowait 和 put_nowait 之间，其他协程填满了队列
                #   - 原因：不可能（锁保证了 Ring Buffer 的原子性）
                #   - 防御性处理：代码健壮性考虑
                #
                # 处理策略：只能丢弃当前事件并记录警告
                self._logger.warning("事件总线队列已满/竞争条件，丢弃事件: %s", event.type)
                # 注意：这里不抛出异常，保持 Best-effort 语义（尽力发布，失败时静默丢弃）

    async def start(self) -> None:
        """启动事件总线（幂等操作）

        该方法启动后台 worker 任务，开始处理队列中的事件。多次调用是安全的（幂等），不会创建多个 worker 任务

        Note:
            幂等性保证：
            - 若总线已启动，直接返回（不会创建重复的 worker）
            - 使用 _is_running 标志判断状态

            启动流程：
            1. 检查运行状态，若已启动则直接返回（幂等性）
            2. 设置运行标志为 True（允许接受新事件）
            3. 创建后台 worker 任务（使用 create_task）
            4. 让出控制权，确保 worker 开始运行（调度到事件循环）

            任务命名：
            - 使用 name="eventbus-worker" 便于调试和监控
            - 可通过 asyncio.all_tasks() 查看所有任务及其名称
        """
        # 幂等性检查：若已启动，直接返回
        if self._is_running:
            return

        # 设置运行标志为 True，此后 publish() 会接受新事件，_worker() 会持续处理事件
        self._is_running = True

        # 创建后台 worker 任务，create_task() 会立即返回任务句柄，任务会在事件循环中异步执行
        # name 参数用于调试，可通过 task.get_name() 获取
        self._worker_task = asyncio.create_task(
            self._worker(), name="eventbus-worker"
        )

        # 让出控制权，确保 worker 开始运行
        # 这不是必需的（worker 最终会被调度），但这样做可以确保：
        # 1. worker 立即开始执行（而不是等到下一次事件循环）
        # 2. 若 worker 启动失败，异常会立即抛出（而不是延迟到后续调用）
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """优雅停止事件总线

        该方法停止接受新事件，并等待队列中的剩余事件处理完毕后关闭 worker 任务。使用哨兵事件模式确保所有已发布的事件都被处理，避免事件丢失

        Note:
            优雅停止策略（Graceful Shutdown）：
            1. 拒绝新事件：设置 _is_running = False，publish() 会拒绝新事件
            2. 发送哨兵事件：发布特殊的 "_eventbus.shutdown" 事件作为队列终止标记
            3. 等待队列排空：worker 会处理完哨兵之前的所有事件，然后退出
            4. 带超时保护：使用 wait_for(timeout=5.0) 避免无限等待

            哨兵事件的优势（vs 直接 cancel 任务）：
            - ✅ 确保所有已发布的事件都被处理（不丢失事件）
            - ✅ 优雅关闭，给处理器时间清理资源
            - ✅ 避免 cancel 导致的 CancelledError 和状态不一致
            - ❌ 需要等待队列排空（可能较慢）

            超时处理：
            - 若 worker 在 5 秒内未退出，可能是某个处理器卡住了
            - 记录警告日志并强制 cancel 任务
            - 5 秒是经验值，可根据实际情况调整

            队列排空（Drain Queue）：
            哨兵事件会排在所有已发布事件之后（FIFO），worker 收到哨兵时：
            - 哨兵之前的所有事件已被处理
            - 队列中没有剩余事件（或仅剩哨兵）
            - 可以安全退出循环
        """
        # 幂等性检查：若未启动，直接返回
        if not self._is_running:
            return

        # 步骤 1: 设置停止标志，拒绝新事件，此后 publish() 会拒绝所有新事件（返回前记录 debug 日志）
        self._is_running = False

        # 步骤 2: 发送哨兵事件并等待 worker 退出
        if self._worker_task is not None:
            # 发送哨兵事件，通知 worker 退出，哨兵会排在所有已发布事件之后（FIFO），确保它们都被处理
            # 使用特殊的事件类型 "_eventbus.shutdown"（以下划线开头，表示内部事件）
            await self._queue.put(Event(type=SHUTDOWN_EVENT))

            # 步骤 3: 等待 worker 处理完剩余事件并退出
            try:
                # 使用 wait_for 设置超时，避免无限等待
                # timeout=5.0 秒是经验值，适合大多数场景，若队列中有大量事件或处理器很慢，可能需要更长的超时
                await asyncio.wait_for(self._worker_task, timeout=5.0)
            except asyncio.TimeoutError:
                # worker 在 5 秒内未退出，可能是某个处理器卡住了（如死锁、无限循环）
                self._logger.warning("事件总线工作线程在关闭期间超时。")
                # 强制取消任务（可能导致处理器中断），cancel() 会在下次事件循环时抛出 CancelledError
                self._worker_task.cancel()
                # 注意：cancel 后应 await task 以确保异常被正确处理
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    current_task = asyncio.current_task()
                    if current_task and current_task.cancelled():
                        raise
            except Exception as e:
                # worker 执行过程中抛出异常（不应该发生，因为 _worker 内部已捕获所有异常）
                # 这通常表示代码有 bug（如 _worker 中的 except Exception 未覆盖某些异常）
                self._logger.error(f"事件总线工作线程在关闭期间出错: {e}")
            finally:
                # 清理任务句柄，允许垃圾回收
                self._worker_task = None

    async def _worker(self) -> None:
        """后台工作任务，持续从队列中取出事件并分发给处理器（内部方法）

        该方法是事件总线的核心，实现了消费者角色。它会持续从队列中取出事件，查找对应的处理器并依次调用，直到收到哨兵事件为止

        Note:
            工作流程：
            1. 从队列中取出事件（阻塞等待，直到有事件或收到哨兵）
            2. 检查是否为哨兵事件（"_eventbus.shutdown"）
               - 若是，退出循环（停止处理）
               - 若否，继续处理
            3. 获取该事件类型的所有处理器
            4. 依次调用处理器（使用 _safe_invoke 确保异常被捕获）

            异常隔离：
            - 单个处理器失败不影响其他处理器（通过 _safe_invoke 实现）
            - 处理器抛出的异常会被捕获并记录日志，不会向上传播
            - 确保事件总线的健壮性（一个有 bug 的处理器不会导致整个系统崩溃）

            处理器执行顺序：
            - 同一事件类型的处理器按注册顺序依次执行（FIFO）
            - 处理器是串行执行的（非并发），确保顺序可预测
            - 若需并发执行，处理器应在内部启动后台任务

            哨兵事件的位置：
            - 哨兵会排在所有已发布事件之后（FIFO）
            - worker 读到哨兵时，说明哨兵之前的所有事件都已处理完毕
            - 此时队列中没有剩余的业务事件，可以安全退出
        """
        # 无限循环，持续处理事件直到收到哨兵
        while True:
            # 从队列中取出事件（阻塞等待），若队列为空，该协程会挂起，让出控制权给其他协程，当有新事件时，事件循环会重新唤醒该协程
            event = await self._queue.get()

            # 检查是否为哨兵事件（停止信号）
            if event.type == SHUTDOWN_EVENT:
                # 收到 Shutdown 信号，退出循环，此时哨兵之前的所有事件都已处理完毕（FIFO 保证），队列中没有剩余的业务事件，可以安全退出
                break

            # 获取该事件类型的所有处理器，创建列表副本，避免在迭代过程中 handlers 被修改（如动态取消订阅）
            handlers = list(self._handlers.get(event.type, ()))

            # 依次调用所有处理器，使用 _safe_invoke 确保单个处理器失败不影响其他处理器
            for handler in handlers:
                await self._safe_invoke(handler, event)

            # 注意：这里没有调用 self._queue.task_done()，因为我们不需要使用 join() 等待队列处理完成，队列的清空由哨兵事件保证

    async def _safe_invoke(self, handler: EventHandler, event: Event) -> None:
        """安全调用事件处理器，捕获并记录所有异常（内部方法）

        该方法封装了事件处理器的调用逻辑，确保单个处理器的异常不会影响其他处理器
        所有异常（包括系统异常）都会被捕获并记录日志，不会向上传播

        Args:
            handler (EventHandler): 事件处理器函数（异步函数）
            event (Event): 要传递给处理器的事件对象

        Note:
            异常处理策略：
            - 捕获所有异常：包括 Exception 和系统异常（如 KeyboardInterrupt）
            - 记录错误日志：包含事件类型、处理器和错误信息
            - 不向上抛出异常：确保单个处理器失败不影响其他处理器

            为何捕获所有异常（包括 BaseException）
            - 虽然通常只捕获 Exception，但这里使用 Exception 而非 BaseException
            - 原因：KeyboardInterrupt、SystemExit 等系统异常应该被允许传播，中断程序
            - 但在事件处理器中，这些异常可能是误用（如在处理器中调用 sys.exit()）
            - 当前实现只捕获 Exception，若需更严格的控制可改为 BaseException

            日志格式：
            - type: 事件类型（便于定位是哪类事件触发的异常）
            - handler: 处理器函数（用 repr 展示，包含模块和函数名）
            - error: 异常对象（包含异常类型和消息）

            noqa 标记：
            - noqa: BLE001 告诉 linter（如 ruff）这里是故意捕获所有异常的
            - 避免 linter 报告 "bare except" 或 "too broad exception" 警告
        """
        try:
            # 调用事件处理器（可能抛出异常）
            await handler(event)
        except Exception as exc:  # noqa: BLE001
            # 捕获所有异常（这里是故意的，不是编码错误）
            self._logger.error(
                "事件处理器执行失败。类型=%s 处理器=%r 错误=%s",
                event.type,
                handler,
                exc
            )
            # 不向上抛出异常，确保：
            # 1. 其他处理器继续执行（异常隔离）
            # 2. worker 循环不会中断（健壮性）
            # 3. 事件总线保持运行（可用性）
