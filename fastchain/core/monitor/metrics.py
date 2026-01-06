from __future__ import annotations

from prometheus_client import Counter, Histogram, Gauge


class MetricsManager:
    """
    Prometheus 指标管理器 (Singleton)。定义所有需要采集的指标
    """

    # 1. HTTP 层面指标 (RED Method)
    http_requests_total = Counter(
        "http_requests_total",
        "Total count of HTTP requests",
        ["method", "endpoint", "status"]
    )

    http_request_duration_seconds = Histogram(
        "http_request_duration_seconds",
        "HTTP request latency in seconds",
        ["method", "endpoint"],
        # 桶设置：针对 LLM 场景优化，包含长尾延迟
        buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
    )

    # 2. 业务层面指标 (LLM Specific)
    llm_requests_total = Counter(
        "llm_requests_total",
        "Total count of LLM invocations",
        ["model", "scene", "status"]
    )

    llm_token_usage_total = Counter(
        "llm_token_usage_total",
        "Total number of tokens used",
        ["model", "scene", "type"]
    )

    llm_request_duration_seconds = Histogram(
        "llm_request_duration_seconds",
        "LLM invocation latency in seconds",
        ["model", "scene"],
        buckets=[1.0, 5.0, 10.0, 30.0, 60.0]
    )

    # 3. 资源层面指标
    active_requests = Gauge(
        "fastchain_active_requests",
        "Number of requests currently being processed"
    )


# 全局单例导出
metrics = MetricsManager()
