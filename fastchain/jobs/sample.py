import asyncio

from loguru import logger

from ..core.config import SettingsManager
from ..core.scheduler.decorators import scheduled


# 1. 使用 @scheduled 装饰器注册任务
# key: 任务唯一标识（用于分布式锁和配置覆盖）
# cron: Cron 表达式 (分 时 日 月 周) -> 这里是 "每分钟的第0秒执行"
# distributed: True 表示启用分布式锁（集群中只有一个节点运行）
@scheduled(key="demo_heartbeat", cron="* * * * *", distributed=True)
async def demo_heartbeat_job(settings: SettingsManager):
    """
    演示任务：分布式心跳 + 配置依赖注入

    依赖注入演示：
    只需在参数中声明 'settings'，调度器会自动注入当前最新的配置管理器实例。
    无需手动 import 全局变量，也无需担心配置过期（SettingsManager 内部连接着热更新的 RuntimeStore）
    """
    # 1. 获取当前命名空间
    ns = settings.local.apollo_namespace

    # 2. 读取动态配置 (假设有一个 'timeout' 配置，默认 30秒)
    # 当 Apollo 推送新配置时，下一次任务执行这里获取到的就是新值！
    timeout = settings.get_value(ns, "llm.timeout", default=30)

    logger.info(f"💓 [Demo Job] 分布式心跳 (当前 LLM 超时配置: {timeout}s)...")

    # 模拟一个异步耗时操作
    await asyncio.sleep(1)

    logger.success("💓 [Demo Job] 任务执行完成！")


# 2. 也可以注册同步函数，框架会自动扔到线程池执行，不会阻塞主线程
# 注意：同步函数暂不支持依赖注入（其实也支持，懒得写，这里保持简单演示）
@scheduled(key="demo_sync_task", cron="*/5 * * * *", distributed=False)
def demo_sync_job():
    """演示任务：每5分钟执行一次的本地同步任务"""
    logger.info("🔄 [Demo Job] 本地同步任务触发")
