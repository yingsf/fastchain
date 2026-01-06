from __future__ import annotations

from typing import Callable, Optional, Any

from pydantic import BaseModel, Field, field_validator


class JobConfig(BaseModel):
    """定时任务配置模型。

    用于描述一个定时任务的完整元数据，既包含代码中装饰器定义的默认值，
    也包含从 Apollo 配置中心加载的覆盖值。该模型是"代码定义 + 配置驱动"混合调度模式的核心数据结构

    设计考量：
    - 使用 Pydantic 进行参数校验，确保配置的类型安全和格式正确性
    - `func` 字段在序列化时被排除（exclude=True），避免函数对象被写入配置文件
    - 支持同步和异步函数，由 SchedulerManager 在运行时自动识别并适配执行方式

    Attributes:
        self.key (str): 任务唯一标识（如 "daily_report"），用于配置覆盖和分布式锁的键名
        self.cron (str): Cron 表达式（如 "0 1 * * *"），遵循标准五段式格式（分 时 日 月 周）
        self.distributed (bool): 是否启用分布式锁。True 表示集群模式下只跑一次，False 表示每个实例独立执行
        self.enabled (bool): 是否启用该任务。用于运行时动态控制任务的开关状态
        self.misfire_grace_time (int): 错过执行时间的容忍窗口（秒），APScheduler 在此时间内仍会执行错过的任务
        self.func (Optional[Callable[..., Any]]): 任务对应的 Python 函数对象（运行时注入，不从配置加载）

    Note:
        该模型实例会在以下两个阶段产生：
        1. 装饰器阶段：从 @scheduled 注解中提取默认配置。
        2. 配置加载阶段：从 Apollo 读取覆盖配置，与代码定义合并后生成最终配置。
    """

    # 任务唯一标识，必填字段，用于日志追踪、分布式锁键名生成、配置覆盖匹配
    key: str = Field(..., description="任务唯一标识")

    # Cron 表达式，必填字段，定义任务的执行时机
    cron: str = Field(..., description="Cron 表达式")

    # 分布式锁开关，默认启用，适用于集群环境下的全局唯一执行
    distributed: bool = Field(True, description="是否启用分布式锁")

    # 任务启用状态，默认启用，可通过配置中心动态控制
    enabled: bool = Field(True, description="是否启用")

    # 容错时间窗口，单位秒，避免因短暂延迟导致任务被跳过
    misfire_grace_time: int = Field(60, description="错过执行的容忍时间(秒)")

    # 函数对象，运行时注入，不参与 Pydantic 序列化（避免函数对象被写入 JSON 配置）
    # exclude=True 是关键设计：函数对象不可序列化，且配置文件中不应包含可执行代码
    func: Optional[Callable[..., Any]] = Field(None, exclude=True)

    @field_validator("cron")
    def validate_cron(cls, v: str) -> str:
        """简单的 Cron 表达式格式校验。

        校验 Cron 表达式是否符合标准五段式格式（分 时 日 月 周）。
        注意：此处仅做基础格式校验，不验证语义合法性（如月份范围）。

        Args:
            v (str): 待校验的 Cron 表达式字符串。

        Returns:
            str: 校验通过后的原始 Cron 表达式。

        Raises:
            ValueError: 当 Cron 表达式不是五段式格式时抛出。

        Note:
            更严格的语义校验（如 "0-59" 范围检查）由 APScheduler 的 CronTrigger 完成。
            此处只做最基本的"是否五段"校验，避免明显格式错误进入调度器。
        """
        # 按空格分割 Cron 表达式，标准格式为五段（分 时 日 月 周）
        parts = v.split()
        if len(parts) != 5:
            # 抛出明确的错误信息，帮助用户快速定位配置问题
            raise ValueError(f"Cron 表达式必须包含 5 个部分 (分 时 日 月 周), 当前: '{v}'")
        return v
