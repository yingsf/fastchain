from __future__ import annotations

import time
from typing import Optional, Set

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from ...core.monitor.metrics import metrics
from ...core.tracing.context import new_trace_id


class TraceIDMiddleware(BaseHTTPMiddleware):
    """TraceID 中间件，负责分布式追踪上下文的初始化与传播

    该中间件在每个 HTTP 请求的生命周期开始时生成唯一的 TraceID，并将其存储到线程安全的 ContextVar 中，以便在整个请求处理链路中
    （包括异步任务、日志记录、下游服务调用）都能访问到统一的追踪标识

    TraceID 会被注入到响应头 X-Trace-ID 中，便于客户端和服务端进行日志关联、调用链追踪和问题诊断

    Attributes:
        app (ASGIApp): 被包装的 ASGI 应用实例

    Note:
        - TraceID 的生成依赖 fastchain.core.tracing.context.new_trace_id()
        - ContextVar 保证了异步上下文的隔离性，不会在并发请求间发生串扰
        - 该中间件应该在中间件栈的较早位置注册，以确保后续中间件和业务逻辑都能访问到 TraceID
    """

    async def dispatch(self, request: Request, call_next):
        """处理单个 HTTP 请求，初始化 TraceID 并注入响应头

        Args:
            request (Request): FastAPI/Starlette 的请求对象
            call_next (callable): 中间件链中的下一个处理器（可能是下一个中间件或路由处理函数）

        Returns:
            Response: 处理后的 HTTP 响应对象，已注入 X-Trace-ID 响应头

        Note:
            执行流程：
            1. 调用 new_trace_id() 生成全局唯一的追踪标识，并存入 ContextVar
            2. 将请求传递给下游处理器（可能经过多层中间件和业务逻辑）
            3. 在响应返回前，将 TraceID 注入到响应头中

            ContextVar 的使用保证了即使在高并发场景下，每个请求的 TraceID也不会相互干扰，同时在异步调用链中保持一致
        """
        # 1. 初始化 TraceID 并存入 ContextVar，为整个请求链路建立追踪上下文
        # new_trace_id() 内部会生成唯一 ID（如 UUID）并设置到线程局部存储中
        trace_id = new_trace_id()

        # 2. 继续处理请求，将控制权交给下游中间件或路由处理器
        # 在整个调用链中，所有异步任务都能通过 ContextVar 访问到同一个 trace_id
        response = await call_next(request)

        # 3. 将 TraceID 注入响应头，方便客户端和日志系统进行调用链关联
        # 客户端可以将此 ID 用于前端日志上报，运维人员可以用它串联整个请求链路
        response.headers["X-Trace-ID"] = trace_id

        return response


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Prometheus 指标采集中间件，自动收集 HTTP RED (Rate, Errors, Duration) 指标

    该中间件在每个 HTTP 请求的生命周期中自动记录以下关键指标：
    - **Rate (速率)**：通过 http_requests_total 记录请求总数（按方法、端点、状态码分组）
    - **Errors (错误)**：通过 status_code 维度区分成功与失败请求
    - **Duration (耗时)**：通过 http_request_duration_seconds 记录请求处理时长

    此外，还维护了 active_requests 指标用于实时监控当前正在处理的请求数量，这对于检测流量峰值、容量规划和负载均衡决策非常重要

    Attributes:
        self.app (ASGIApp): 被包装的 ASGI 应用实例
        self.exclude_paths (Set[str]): 需要排除的路径集合，这些路径不会被计入指标统计
            默认排除 /metrics（避免监控端点自己产生指标）、/health（健康检查通常不计入业务指标）和 /favicon.ico（浏览器自动请求）

    Note:
        - 使用 time.perf_counter() 而非 time.time()，因为前者不受系统时钟调整影响，测量相对时间差更精准（亚毫秒级精度）
        - 对于无法匹配到路由的请求，使用 "<unmatched>" 作为端点标签，这是高基数防御策略：
            防止恶意请求通过大量随机 URL 制造无限多的指标维度，导致 Prometheus 内存爆炸（每个唯一的 label 组合会创建一个新的时间序列）
        - finally 块确保即使请求处理失败，指标也能被正确记录，这对于监控系统的完整性至关重要

    Warning:
        - 如果业务中有动态路由参数（如 /user/{user_id}），应确保使用 route.path 而非 request.url.path，
          否则每个不同的 user_id 都会产生一个新的指标维度，造成高基数问题。
        - exclude_paths 应根据实际业务调整，避免将真实业务端点误排除。
    """

    def __init__(self, app: ASGIApp, exclude_paths: Optional[list[str]] = None):
        """初始化 Prometheus 中间件

        Args:
            app (ASGIApp): 要包装的 ASGI 应用实例
            exclude_paths (Optional[list[str]]): 需要排除的路径列表，这些路径的请求不会被计入指标
                默认为 None，此时会使用预设的排除列表 ["/metrics", "/health", "/favicon.ico"]

        Note:
            使用 set 存储排除路径是为了 O(1) 查找性能，避免在高并发场景下使用 list 的 O(n) 查找开销影响请求处理延迟
        """
        super().__init__(app)
        # 将排除路径转换为 set，利用哈希查找的 O(1) 性能，避免每次请求都遍历 list
        # 默认排除监控端点、健康检查和浏览器图标请求，这些不应计入业务指标
        self.exclude_paths: Set[str] = set(exclude_paths or ["/metrics", "/health", "/favicon.ico"])

    async def dispatch(self, request: Request, call_next):
        """处理单个 HTTP 请求，自动记录 RED 指标

        Args:
            request (Request): FastAPI/Starlette 的请求对象
            call_next (callable): 中间件链中的下一个处理器

        Returns:
            Response: 处理后的 HTTP 响应对象

        Note:
            执行流程：
            1. 如果请求路径在排除列表中，直接跳过指标记录，避免监控端点产生递归指标
            2. 记录请求开始时间（使用高精度计时器）
            3. 将活跃请求计数器 +1（用于监控当前并发请求数）
            4. 调用下游处理器并捕获响应状态码
            5. 无论成功或失败，在 finally 块中：
               - 将活跃请求计数器 -1
               - 计算请求耗时
               - 记录请求总数和耗时分布到 Prometheus 指标中

            高基数防御：
            - 对于无法匹配到路由的请求（如随机扫描、恶意探测），统一使用 "<unmatched>" 标签
            - 避免将动态 URL 参数（如 /user/12345）直接作为标签，而是使用路由模板（如 /user/{user_id}）

        Warning:
            - 即使请求处理抛出异常，finally 块仍会执行，确保指标完整性
            - 默认将未捕获异常的状态码设为 500，符合 HTTP 规范
        """
        # 如果请求路径在排除列表中，直接放行，不进行任何指标记录
        # 这避免了 /metrics 端点被 Prometheus 抓取时产生递归指标，以及健康检查产生噪音
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        # 记录请求开始时间，使用 perf_counter 而非 time.time()
        # perf_counter 不受系统时钟调整影响，适合测量短时间间隔（精度更高，通常达到纳秒级）
        start_time = time.perf_counter()
        method = request.method

        # 活跃请求数 +1，用于监控当前正在处理的并发请求数量
        # 这个指标对于检测流量突增、容量规划和负载均衡决策非常重要
        metrics.active_requests.inc()

        # 默认状态码设为 500，如果后续处理成功会被覆盖
        # 这确保即使在异常情况下（如中间件链断裂），也能记录一个合理的状态码
        status_code = "500"
        try:
            # 将请求传递给下游处理器（可能是其他中间件或路由处理函数）
            response = await call_next(request)
            # 获取实际的 HTTP 状态码（如 200, 404, 500 等）
            status_code = str(response.status_code)
            return response
        finally:
            # finally 块确保无论请求成功或失败，指标都能被正确记录
            # 这对于监控系统的完整性至关重要，避免因异常导致指标缺失

            # 活跃请求数 -1，与前面的 inc() 对应
            metrics.active_requests.dec()

            # 计算请求处理耗时（秒），精度达到亚毫秒级
            duration = time.perf_counter() - start_time

            # 安全获取路由对象
            route = getattr(request, "route", None)

            if route:
                # 使用路由模板（如 /user/{user_id}）而非实际 URL（如 /user/12345）
                endpoint = route.path
            else:
                # 对于无法匹配到路由的请求，使用固定标签 "<unmatched>"
                endpoint = "<unmatched>"

            # 记录请求总数，按方法、端点、状态码三个维度分组
            metrics.http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status=status_code
            ).inc()

            # 记录请求耗时分布，按方法、端点分组
            metrics.http_request_duration_seconds.labels(
                method=method,
                endpoint=endpoint
            ).observe(duration)
