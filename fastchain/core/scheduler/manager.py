from __future__ import annotations

import asyncio
import importlib
import inspect
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Any, Protocol, runtime_checkable, Tuple
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from .decorators import get_registered_jobs
from ..config import SettingsManager
from ..config.constants import ResourceName, ResourceKind
from ..resources.base import Resource, ResourceHealth, HealthStatus
from ..resources.events import Event, EventBus, EventConstants

# 默认时区常量 - 所有调度任务使用统一时区以避免跨时区时间计算混乱
DEFAULT_TIMEZONE_STR = "Asia/Shanghai"


@runtime_checkable
class RedisManagerLike(Protocol):
    """Redis 管理器接口协议（鸭子类型）

    定义分布式任务调度所需的最小 Redis 能力集，通过 Protocol 实现依赖倒置，使 SchedulerManager 与具体 Redis 实现解耦

    Methods:
        acquire_lock: 尝试获取分布式锁
        release_lock: 释放已持有的分布式锁

    Note:
        使用 @runtime_checkable 装饰器，支持运行时 isinstance 检查
    """

    async def acquire_lock(self, key: str, token: str, ttl_ms: int = 30000) -> bool:
        """尝试获取分布式锁

        Args:
            key (str): 锁的唯一标识符
            token (str): 锁的持有者标识（用于安全释放）
            ttl_ms (int): 锁的过期时间（毫秒）。默认为 30000

        Returns:
            bool: 获取成功返回 True，失败返回 False
        """
        ...

    async def release_lock(self, key: str, token: str) -> bool:
        """释放已持有的分布式锁

        Args:
            key (str): 锁的唯一标识符
            token (str): 锁的持有者标识（必须与获取时一致）

        Returns:
            bool: 释放成功返回 True，失败返回 False
        """
        ...


@dataclass(frozen=True)
class JobPlan:
    """任务调度计划（纯数据对象，不可变）

    用于在配置解析阶段（线程池中）构建任务调度计划，然后在主线程中原子应用到 APScheduler

    Attributes:
        key (str): 任务唯一标识符
        func (Any): 任务执行函数（可调用对象）
        trigger (CronTrigger): APScheduler Cron 触发器对象
        replace_existing (bool): 是否替换同名已存在任务
        misfire_grace_time (int): 错过执行窗口的宽限时间（秒）
        distributed (bool): 是否需要分布式锁协调。
    """
    key: str
    func: Any
    trigger: CronTrigger
    replace_existing: bool
    misfire_grace_time: int
    distributed: bool


@dataclass(frozen=True)
class ReloadRuntime:
    """重载运行时配置（纯数据对象，不可变）

    封装热重载过程中需要原子切换的运行时参数

    Attributes:
        lock_ttl_ms (int): 分布式锁的 TTL（毫秒）
        timezone (ZoneInfo): 调度器使用的时区对象
    """
    lock_ttl_ms: int
    timezone: ZoneInfo


class SchedulerManager(Resource):
    """分布式任务调度管理器（基于 APScheduler）

    核心职责：
    - 从配置中心（Apollo）动态加载和热重载任务调度配置
    - 使用 APScheduler 执行定时任务
    - 通过 Redis 分布式锁协调多实例任务执行，避免重复触发
    - 自动注入 SettingsManager 到任务函数，支持任务内访问最新配置

    设计考量：
    - 配置解析在线程池中执行，避免阻塞异步事件循环
    - 使用纯数据对象（JobPlan、ReloadRuntime）实现计划-应用分离，确保线程安全
    - 支持热重载合并（debounce），避免频繁配置更新导致重复重载
    - 对于分布式任务，优雅降级到本地执行（当 Redis 不可用时）

    Attributes:
        settings (SettingsManager): 配置管理器（用于访问 Apollo 配置）
        scheduler (AsyncIOScheduler): APScheduler 异步调度器实例
        _event_bus (EventBus | None): 事件总线（用于订阅配置更新事件）
        _redis_manager (RedisManagerLike | None): Redis 管理器（用于分布式锁）
        _timezone (ZoneInfo): 当前调度器时区
        _lock_ttl_ms (int): 分布式锁 TTL（毫秒）
        _last_health_error (str | None): 最后一次健康检查错误信息
        _signature_cache (Dict[Any, inspect.Signature]): 函数签名缓存（避免重复反射）
        _reload_lock (asyncio.Lock): 重载逻辑的异步锁（防止并发重载）
        _reload_event (asyncio.Event): 重载信号事件（用于合并重载）

    Methods:
        set_redis_manager: 注入 Redis 管理器
        start: 启动调度器（订阅配置更新、首次加载任务）
        stop: 停止调度器
        health_check: 健康检查
    """

    def __init__(self, settings: SettingsManager, event_bus: EventBus | None):
        """初始化调度管理器

        Args:
            settings (SettingsManager): 配置管理器实例
            event_bus (EventBus | None): 事件总线实例（可选）
        """
        super().__init__(
            name=ResourceName.SCHEDULER,
            kind=ResourceKind.COMPONENT,
            startup_priority=90
        )
        self.settings = settings
        self._event_bus = event_bus

        # 默认时区和分布式锁 TTL（后续可通过配置覆盖）
        self._timezone = ZoneInfo(DEFAULT_TIMEZONE_STR)
        self._lock_ttl_ms = 60000  # 默认 60 秒

        # 创建 APScheduler 异步调度器实例
        # 使用 AsyncIOScheduler 确保在 asyncio 事件循环中正确执行
        self.scheduler = AsyncIOScheduler(timezone=self._timezone)

        self._redis_manager: RedisManagerLike | None = None
        self._last_health_error: str | None = None

        # 函数签名缓存：避免在每次任务执行时重复反射，提升性能
        self._signature_cache: Dict[Any, inspect.Signature] = {}

        # 重载控制：使用异步锁和事件实现 debounce 机制
        # 如果配置短时间内多次更新，只执行最后一次重载
        self._reload_lock = asyncio.Lock()
        self._reload_event = asyncio.Event()

    def set_redis_manager(self, redis_manager: Any) -> None:
        """注入 Redis 管理器（依赖注入）

        Args:
            redis_manager (Any): Redis 管理器实例（需符合 RedisManagerLike 协议）

        Note:
            使用协议类型检查（Protocol），而非硬编码具体类，实现依赖倒置
        """
        # 运行时检查是否符合 RedisManagerLike 协议
        if isinstance(redis_manager, RedisManagerLike):
            self._redis_manager = redis_manager
            logger.debug("RedisManager 已注入到 SchedulerManager")
        else:
            logger.warning(f"注入的 RedisManager 不符合协议要求: {type(redis_manager)}")

    async def start(self) -> None:
        """启动调度管理器

        执行流程：
        1. 订阅配置更新事件（如果 EventBus 可用）
        2. 首次加载任务配置并注册到 APScheduler
        3. 启动 APScheduler 后台线程

        Raises:
            Exception: 如果任务加载或调度器启动失败
        """
        logger.info("正在启动 SchedulerManager...")

        # 订阅配置更新事件，实现配置热重载
        if self._event_bus:
            self._event_bus.subscribe(EventConstants.CONFIG_UPDATED, self._on_config_updated)

        # 首次加载任务配置（同步执行，确保启动时任务已就绪）
        await self._reload_jobs()

        # 启动 APScheduler 后台调度线程
        self.scheduler.start()
        logger.success("SchedulerManager 已启动")

    async def stop(self) -> None:
        """停止调度管理器

        优雅关闭 APScheduler，等待正在执行的任务完成（取决于 wait 参数）
        """
        logger.info("正在停止 SchedulerManager...")
        # wait=False: 立即停止调度，不等待正在执行的任务，对于长时间运行的任务，可能需要配合外部超时机制
        self.scheduler.shutdown(wait=False)

    async def health_check(self) -> ResourceHealth:
        """执行健康检查

        检查项：
        - APScheduler 是否正在运行
        - 当前注册的任务数量
        - Redis 是否可用（如果已注入）

        Returns:
            ResourceHealth: 健康检查结果对象

        Note:
            使用 try-except 捕获所有异常，防止健康检查本身导致系统崩溃
        """
        running = self.scheduler.running
        jobs_count = 0
        status = HealthStatus.HEALTHY
        details = {"redis_enabled": self._redis_manager is not None}

        try:
            # 获取当前调度器中的任务数量
            jobs_count = len(self.scheduler.get_jobs())
        except Exception as e:
            self._last_health_error = str(e)
            status = HealthStatus.UNHEALTHY
            logger.error(f"调度器健康检查失败: {e}")

        # 如果调度器未运行，标记为 DOWN
        if not running:
            status = HealthStatus.DOWN

        return ResourceHealth(
            kind=self.kind, name=self.name, status=status,
            message=f"Jobs: {jobs_count}",
            last_error=self._last_health_error,
            details={**details, "jobs_count": jobs_count}
        )

    async def _on_config_updated(self, event: Event) -> None:
        """配置更新事件回调函数

        当配置中心推送配置更新时触发，自动重载任务配置

        Args:
            event (Event): 配置更新事件对象

        Note:
            使用命名空间过滤，避免无关配置更新触发重载
        """
        # 从事件负载中提取更新的命名空间列表
        updated_ns = event.payload.get(EventConstants.KEY_UPDATED_NAMESPACES, [])
        current_ns = self.settings.local.apollo_namespace

        # 如果更新的命名空间不包含当前应用的命名空间，跳过重载
        if isinstance(updated_ns, list) and current_ns not in updated_ns:
            return

        logger.info(f"检测到配置更新 (涉及: {updated_ns})，正在刷新定时任务...")
        try:
            # 异步重载任务配置
            await self._reload_jobs()
        except Exception as e:
            logger.error(f"热重载定时任务失败: {e}")

    async def _reload_jobs(self) -> None:
        """重载任务配置（支持 debounce 合并）

        工作流程：
        1. 尝试获取重载锁（如果锁已被持有，设置 reload_event 并返回）
        2. 在持有锁期间，循环执行重载逻辑，直到没有新的重载信号
        3. 每次重载：
           - 构建任务计划（在线程池中）
           - 应用任务计划到 APScheduler（在主线程中）

        设计考量：
        - 使用异步锁防止并发重载导致状态不一致
        - 使用 Event 机制实现 debounce：如果重载期间又有新的配置更新，合并到下一轮重载中，避免频繁重启调度器
        """
        # 如果已有重载在进行中，设置挂起信号并返回，当前重载完成后会检查该信号，并立即执行合并重载
        if self._reload_lock.locked():
            self._reload_event.set()
            return

        async with self._reload_lock:
            # 循环执行重载，直到没有挂起的信号
            while True:
                self._reload_event.clear()

                try:
                    # 获取当前配置快照（原子读取，避免重载过程中配置被修改）
                    snapshot = self.settings.get_snapshot()

                    # [Plan Phase] 在线程池中构建任务计划
                    # 配置解析涉及大量字符串处理和字典操作，放到线程池避免阻塞事件循环
                    plans, runtime = await asyncio.to_thread(self._build_job_plans, snapshot)

                    # [Apply Phase] 在主线程中应用任务计划
                    # APScheduler 的 API 不是线程安全的，必须在主线程调用
                    self._apply_job_plans(plans, runtime)

                except Exception as e:
                    logger.error(f"任务重载过程中发生错误: {e}")

                # 检查是否有挂起的重载信号，如果没有，退出循环；如果有，继续下一轮重载
                if not self._reload_event.is_set():
                    break
                logger.debug("检测到挂起的重载信号，立即执行合并重载...")

    def _build_job_plans(self, config_snapshot: Any) -> Tuple[List[JobPlan], ReloadRuntime]:
        """[Plan Phase] 构建任务计划（运行于线程池）

        从配置快照中解析任务定义，构建任务计划列表。该方法在独立线程中执行，避免阻塞主事件循环

        Args:
            config_snapshot (Any): 配置快照对象

        Returns:
            Tuple[List[JobPlan], ReloadRuntime]: 任务计划列表和运行时配置

        Note:
            所有 I/O 和 CPU 密集型操作（配置解析、字符串分割、Cron 解析），都在此方法中完成，确保主线程只负责状态更新
        """
        # 解析全局调度器配置和任务配置
        scheduler_conf, jobs_conf = self._parse_global_config(config_snapshot)

        # 构建运行时配置（时区、锁 TTL 等）
        runtime = self._build_runtime_config(scheduler_conf)

        # 如果调度器全局开关关闭，返回空任务列表
        if not bool(scheduler_conf.get("enabled", True)):
            return [], runtime

        # 扫描配置中指定的任务模块（触发 @schedule 装饰器注册）
        self._scan_configured_modules(scheduler_conf)

        # 获取所有通过装饰器注册的任务
        raw_jobs = get_registered_jobs()

        # 合并任务配置覆盖（支持新旧两种配置结构）
        overrides = self._merge_job_overrides(scheduler_conf, jobs_conf)

        # 生成最终的任务计划列表
        plans = self._generate_job_plans(raw_jobs, overrides, runtime.timezone)

        return plans, runtime

    def _parse_global_config(self, config_snapshot: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """解析全局配置

        从配置快照中提取调度器配置和任务配置

        Args:
            config_snapshot (Any): 配置快照对象

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: (调度器配置, 任务配置)

        Note:
            由于在 app.py 中已注册配置转换器，这里拿到的 system.config 和 jobs.config 都已经是 Python dict 对象，无需手动JSON解析
        """
        ns = self.settings.local.apollo_namespace
        sys_conf_section = config_snapshot.sections.get(ns)

        sys_conf: Dict[str, Any] = {}
        jobs_conf: Dict[str, Any] = {}

        if sys_conf_section:
            # 直接获取已转换的字典对象（转换器已在 SettingsManager 中完成解析）
            val = sys_conf_section.values.get("system.config")
            if isinstance(val, dict):
                sys_conf = val

            # 直接获取已转换的字典对象
            jobs_val = sys_conf_section.values.get("jobs.config")
            if isinstance(jobs_val, dict):
                jobs_conf = jobs_val

        # 提取调度器子配置
        scheduler_conf = sys_conf.get("scheduler", {})
        if not isinstance(scheduler_conf, dict):
            scheduler_conf = {}

        return scheduler_conf, jobs_conf

    def _build_runtime_config(self, scheduler_conf: Dict[str, Any]) -> ReloadRuntime:
        """构建运行时配置对象

        Args:
            scheduler_conf (Dict[str, Any]): 调度器配置字典

        Returns:
            ReloadRuntime: 运行时配置对象

        Note:
            时区解析失败时回退到默认时区，避免因配置错误导致调度器无法启动
        """
        # 读取分布式锁 TTL（毫秒），默认 60 秒
        lock_ttl_ms = int(scheduler_conf.get("lock_ttl_ms", 60000))

        # 读取时区配置，默认为上海时区
        tz_str = scheduler_conf.get("timezone", DEFAULT_TIMEZONE_STR)
        try:
            timezone = ZoneInfo(tz_str)
        except Exception as e:
            logger.warning(f"无效的时区 '{tz_str}'，回退到默认: {e}")
            timezone = ZoneInfo(DEFAULT_TIMEZONE_STR)

        return ReloadRuntime(lock_ttl_ms=lock_ttl_ms, timezone=timezone)

    def _scan_configured_modules(self, scheduler_conf: Dict[str, Any]) -> None:
        """扫描配置中指定的任务模块

        触发模块导入，使得模块中通过 @schedule 装饰器注册的任务生效

        Args:
            scheduler_conf (Dict[str, Any]): 调度器配置字典

        Note:
            仅导入模块，不调用任何函数。装饰器在模块导入时自动执行
        """
        modules_to_scan = scheduler_conf.get("modules", [])
        if isinstance(modules_to_scan, list) and modules_to_scan:
            self._scan_modules(modules_to_scan)

    def _merge_job_overrides(
            self, scheduler_conf: Dict[str, Any], jobs_conf: Dict[str, Any]
    ) -> Dict[str, Any]:
        """合并任务配置覆盖

        支持两种配置结构：
        - 旧结构：system.config.scheduler.jobs
        - 新结构：jobs.config.schedules

        新结构优先级更高（后合并）

        Args:
            scheduler_conf (Dict[str, Any]): 调度器配置字典
            jobs_conf (Dict[str, Any]): 任务配置字典

        Returns:
            Dict[str, Any]: 合并后的任务配置覆盖字典

        Note:
            由于转换器已在配置存储阶段完成解析，这里无需手动 JSON 解析
        """
        # 旧结构：system.config.scheduler.jobs
        legacy_overrides = scheduler_conf.get("jobs", {})
        if not isinstance(legacy_overrides, dict):
            legacy_overrides = {}

        # 新结构：jobs.config.schedules
        new_overrides = jobs_conf.get("schedules", {})
        if not isinstance(new_overrides, dict):
            new_overrides = {}

        # 合并配置（新结构覆盖旧结构）
        overrides: Dict[str, Any] = dict(legacy_overrides)
        overrides.update(new_overrides)
        return overrides

    def _generate_job_plans(
            self, raw_jobs: List[Any], overrides: Dict[str, Any], timezone: ZoneInfo
    ) -> List[JobPlan]:
        """生成任务计划列表

        将装饰器注册的任务与配置覆盖合并，生成最终的任务计划

        Args:
            raw_jobs (List[Any]): 装饰器注册的原始任务列表
            overrides (Dict[str, Any]): 任务配置覆盖字典
            timezone (ZoneInfo): 时区对象

        Returns:
            List[JobPlan]: 任务计划列表

        Note:
            配置覆盖允许动态修改任务的 cron 表达式、启用状态和分布式锁设置，无需修改代码即可调整调度策略
        """
        plans: List[JobPlan] = []
        for job_cfg in raw_jobs:
            # 获取该任务的配置覆盖
            override_cfg = overrides.get(job_cfg.key, {})
            if not isinstance(override_cfg, dict):
                override_cfg = {}

            # 合并配置：配置覆盖 > 装饰器默认值
            final_cron = override_cfg.get("cron", job_cfg.cron)
            final_enabled = override_cfg.get("enabled", job_cfg.enabled)
            final_distributed = override_cfg.get("distributed", job_cfg.distributed)

            # 如果任务被禁用，跳过
            if not bool(final_enabled):
                continue

            # 解析 Cron 表达式
            trigger = self._parse_trigger(str(final_cron), timezone)
            if not trigger:
                continue

            # 构建任务计划对象
            plans.append(
                JobPlan(
                    key=job_cfg.key,
                    func=job_cfg.func,
                    trigger=trigger,
                    replace_existing=True,
                    misfire_grace_time=job_cfg.misfire_grace_time,
                    distributed=bool(final_distributed),
                )
            )
        return plans

    def _apply_job_plans(self, plans: List[JobPlan], runtime: ReloadRuntime) -> None:
        """[Apply Phase] 应用任务计划到 APScheduler

        在主线程中原子更新调度器状态：
        1. 更新运行时配置（时区、锁 TTL）
        2. 清空现有任务
        3. 注册新任务

        Args:
            plans (List[JobPlan]): 任务计划列表
            runtime (ReloadRuntime): 运行时配置对象

        Note:
            APScheduler 的 API 不是线程安全的，必须在主线程调用
            使用 remove_all_jobs 确保旧配置不残留，避免任务重复执行
        """
        # 更新分布式锁 TTL
        self._lock_ttl_ms = runtime.lock_ttl_ms

        # 检查时区是否变化，如果变化则记录日志
        if self._timezone != runtime.timezone:
            logger.info(f"调度器时区已更新: {self._timezone} -> {runtime.timezone}")
            self._timezone = runtime.timezone

        # 清空所有现有任务（避免旧配置残留）
        self.scheduler.remove_all_jobs()

        # 如果没有有效任务，记录警告并返回
        if not plans:
            logger.warning("Scheduler 全局开关已关闭或无有效任务，已清理所有 Job")
            return

        # 注册所有新任务
        for plan in plans:
            # 为每个任务创建独立的入口函数，捕获当前循环变量，使用默认参数捕获循环变量，避免闭包延迟绑定问题
            async def job_entry_point(k=plan.key, f=plan.func, d=plan.distributed):
                await self._job_wrapper(k, f, d)

            try:
                # 向 APScheduler 注册任务
                self.scheduler.add_job(
                    job_entry_point,
                    trigger=plan.trigger,
                    id=plan.key,
                    replace_existing=plan.replace_existing,
                    misfire_grace_time=plan.misfire_grace_time
                )
                logger.debug(f"任务已调度: {plan.key} -> {plan.trigger}")
            except Exception as e:
                logger.error(f"调度任务 '{plan.key}' 失败: {e}")

    def _scan_modules(self, modules: List[str]) -> None:
        """扫描并导入任务模块

        Args:
            modules (List[str]): 模块名称列表

        Note:
            跳过以下划线或点开头的模块名（私有模块或相对导入），以及包含路径分隔符的模块名（非标准模块路径）
        """
        for mod_name in modules:
            # 跳过私有模块和非法模块名
            if mod_name.startswith(("_", ".")) or "/" in mod_name:
                continue
            try:
                # 动态导入模块（触发装饰器执行）
                importlib.import_module(mod_name)
            except ImportError as e:
                logger.exception(f"无法导入任务模块 '{mod_name}': {e}")

    def _parse_trigger(self, cron_expression: str, timezone: ZoneInfo) -> CronTrigger | None:
        """解析 Cron 表达式为 APScheduler 触发器

        Args:
            cron_expression (str): 标准 5 段式 Cron 表达式（分 时 日 月 周）
            timezone (ZoneInfo): 时区对象

        Returns:
            CronTrigger | None: 解析成功返回触发器对象，失败返回 None

        Note:
            严格校验 Cron 表达式长度（必须为 5 段），防止配置错误导致调度异常
        """
        try:
            # 分割 Cron 表达式
            vals = cron_expression.split()
            if len(vals) != 5:
                raise ValueError(f"Cron 长度错误: {len(vals)} != 5")

            # 构建 APScheduler CronTrigger
            return CronTrigger(
                minute=vals[0], hour=vals[1], day=vals[2], month=vals[3], day_of_week=vals[4],
                timezone=timezone
            )
        except Exception as e:
            logger.error(f"Cron 表达式解析失败 '{cron_expression}': {e}")
            return None

    async def _job_wrapper(self, key: str, user_func: Any, distributed: bool):
        """任务执行包装器（处理分布式锁和异常）

        工作流程：
        1. 如果不需要分布式锁，直接执行任务
        2. 如果需要分布式锁但 Redis 不可用，降级为本地执行并记录警告
        3. 如果需要分布式锁且 Redis 可用：
           - 尝试获取锁
           - 如果获取成功，执行任务并释放锁
           - 如果获取失败，跳过本次执行（其他实例正在执行）

        Args:
            key (str): 任务唯一标识符
            user_func (Any): 用户定义的任务函数
            distributed (bool): 是否需要分布式锁

        Note:
            分布式锁的 TTL 必须大于任务的最大执行时间，否则可能出现锁过期导致任务重复执行
            当 Redis 不可用时，自动降级到本地执行，保证任务不会因为依赖故障而完全停止
        """
        # 如果不需要分布式锁，直接执行
        if not distributed:
            await self._safe_execute(key, user_func)
            return

        # 如果需要分布式锁但 Redis 未就绪，降级为本地执行
        if not self._redis_manager:
            logger.warning(f"任务 '{key}' 要求分布式锁，但 RedisManager 未就绪。降级为本地执行!")
            await self._safe_execute(key, user_func)
            return

        # 构建分布式锁的 key 和 token
        lock_key = f"fastchain:lock:job:{key}"
        token = str(uuid.uuid4())  # 使用 UUID 作为锁的持有者标识

        try:
            # 尝试获取分布式锁
            if await self._redis_manager.acquire_lock(lock_key, token, ttl_ms=self._lock_ttl_ms):
                logger.info(f"任务 '{key}' 抢锁成功，开始执行...")
                try:
                    # 执行任务
                    await self._safe_execute(key, user_func)
                finally:
                    # 无论任务成功或失败，都尝试释放锁
                    try:
                        await self._redis_manager.release_lock(lock_key, token)
                    except Exception as e:
                        logger.error(f"任务 '{key}' 释放锁失败: {e}")
            else:
                # 锁被其他实例持有，跳过本次执行
                logger.debug(f"任务 '{key}' 锁定中，跳过本次执行")
        except Exception as e:
            logger.error(f"任务 '{key}' 分布式锁逻辑异常: {e}")

    def _get_signature(self, func: Any) -> inspect.Signature:
        """获取函数签名（带缓存）

        使用缓存避免重复反射，提升性能

        Args:
            func (Any): 函数对象

        Returns:
            inspect.Signature: 函数签名对象

        Note:
            反射操作（inspect.signature）在 Python 中性能开销较大，缓存签名可以显著提升高频调用场景（如定时任务）的性能
        """
        if func not in self._signature_cache:
            self._signature_cache[func] = inspect.signature(func)
        return self._signature_cache[func]

    async def _safe_execute(self, key: str, func: Any):
        """安全执行任务函数（支持参数注入、异常捕获和事件发布）

        工作流程：
        1. 检查函数签名，自动注入 SettingsManager（如果需要）
        2. 发布任务启动事件
        3. 执行任务函数（自动识别同步/异步）
        4. 发布任务完成/失败事件

        Args:
            key (str): 任务唯一标识符
            func (Any): 任务函数（可调用对象）

        Note:
            支持自动注入 SettingsManager，使任务函数可以访问最新配置，无需手动传参或使用全局变量
            对于同步函数，自动卸载到线程池执行，避免阻塞事件循环
        """
        # 记录任务开始时间
        start_time = time.perf_counter()
        call_kwargs = {}

        try:
            # 获取函数签名
            sig = self._get_signature(func)

            # 遍历参数，自动注入 SettingsManager
            for name, param in sig.parameters.items():
                if name == "settings":
                    # 检查参数类型是否支持关键字传参
                    if param.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                        call_kwargs["settings"] = self.settings
                    else:
                        logger.warning(f"任务 '{key}' 的 'settings' 参数无法注入 (类型不兼容: {param.kind})")
                elif param.default == inspect.Parameter.empty and param.kind in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        inspect.Parameter.KEYWORD_ONLY
                ):
                    # 如果存在其他必填参数，拒绝执行（调度器只支持注入 settings）
                    raise TypeError(f"任务函数 '{key}' 缺少必填参数 '{name}'，调度器仅支持自动注入 'settings'")
        except Exception as e:
            logger.error(f"任务 '{key}' 签名检查未通过，取消执行: {e}")
            await self._publish_job_event(
                EventConstants.JOB_FAILED, key,
                {"status": "signature_error", "error": str(e)}
            )
            return

        # 发布任务启动事件
        await self._publish_job_event(EventConstants.JOB_STARTED, key, {})

        try:
            # 判断任务函数是同步还是异步，并执行
            if asyncio.iscoroutinefunction(func):
                # 异步函数直接 await 执行
                await func(**call_kwargs)
            else:
                # 同步函数卸载到线程池执行，避免阻塞事件循环
                logger.debug(f"任务 '{key}' 检测为同步函数，已自动卸载至线程池执行")
                await asyncio.to_thread(func, **call_kwargs)

            # 计算任务执行时长（毫秒）
            duration = (time.perf_counter() - start_time) * 1000
            await self._publish_job_completion_event(key, duration, True)
        except Exception as e:
            logger.error(f"任务 '{key}' 执行出错: {e}")
            duration = (time.perf_counter() - start_time) * 1000
            await self._publish_job_completion_event(key, duration, False)

    async def _publish_job_event(self, event_type: str, key: str, extra_payload: Dict[str, Any]) -> None:
        """发布任务事件

        Args:
            event_type (str): 事件类型（如 JOB_STARTED, JOB_FAILED）
            key (str): 任务唯一标识符
            extra_payload (Dict[str, Any]): 额外的事件负载

        Note:
            事件发布是异步操作，但不会阻塞任务执行（fire-and-forget）
        """
        if self._event_bus:
            payload = {
                EventConstants.KEY_JOB_ID: key,
                EventConstants.KEY_TIMESTAMP: time.time(),
                **extra_payload
            }
            await self._event_bus.publish(Event(type=event_type, payload=payload))

    async def _publish_job_completion_event(self, key: str, duration: float, success: bool) -> None:
        """发布任务完成事件

        Args:
            key (str): 任务唯一标识符
            duration (float): 任务执行时长（毫秒）
            success (bool): 任务是否执行成功
        """
        if success:
            await self._publish_job_event(
                EventConstants.JOB_COMPLETED, key, {"duration_ms": duration, "status": "success"}
            )
        else:
            await self._publish_job_event(
                EventConstants.JOB_FAILED, key,
                {"duration_ms": duration, "status": "error", "error": "执行异常，详见日志"}
            )
