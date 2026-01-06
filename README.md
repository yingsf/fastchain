# FastChain

[![PyPI version](https://img.shields.io/pypi/v/fastchain-cucc.svg)](https://pypi.org/project/fastchain-cucc/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


> **PyPI 包名**：`fastchain-cucc` 
> **Python 导入名**：`fastchain` 
> **定位**：团队内部 FastAPI + LLM 工程框架（配置中心驱动 + 资源生命周期 + 模块化装配），并提供CLI 初始化能力。

---

## 你会得到什么

FastChain 的目标是：用 **“Apollo 配置驱动 + 自动发现 + 资源生命周期”** 把一个可扩展的 AI 应用工程快速跑起来。

### 核心特性（你最常用的部分）

- **Apollo 强依赖的配置引导（Fail-fast）**
  - 启动阶段从 Apollo 拉取配置；必要配置缺失会直接中止启动，避免“带病运行”。
- **配置驱动的模块装配**
  - `system.config.resources.modules`：启用哪些“资源模块”（DB/Redis/Mongo/LLM/Prompt/Scheduler…）
  - `system.config.routers.modules`：启用哪些“路由模块”（自动扫描导出的 `router`）
- **资源（Resource）生命周期管理**
  - 统一 `start/stop/health_check`、分层启动、后台 watcher（例如 Apollo watcher/DB health watcher）。
- **内置资源（可按需启用）**
  - relational（SQLite 默认可跑）
  - MongoDB、Redis
  - LLM（必需资源：启动 wiring 阶段会要求存在）
  - Prompt（从 Apollo 扫描 `prompts.*` 的 YAML 文本）
  - Scheduler（Job 定时任务）

---

## 安装

推荐使用 `uv`：

```bash
uv pip install fastchain-cucc
```

验证安装：

```bash
fastchain version
```

------

## 3 分钟跑起来（推荐流程）

下面以工程名 `test_fastchain` 为例进行说明。

### 1）创建工程目录 + venv + 安装依赖

```bash
mkdir test_fastchain
cd test_fastchain

uv venv
source .venv/bin/activate

uv pip install fastchain-cucc
```

### 2）在工程根目录就地初始化

> `--apollo-url` 必填（因为 Apollo 是 Fastchain 强依赖）

```bash
fastchain init --apollo-url "http://your-apollo:8080"
```

初始化后结构类似：

```bash
test_fastchain/
├── config/
│   ├── settings.toml
│   └── apollo_example/
│       ├── system.config.json
│       ├── llm.models.json
│       ├── jobs.config.json
│       ├── prompts.default_extraction.yaml
│       └── README.md
├── data/
├── log/
└── test_fastchain/
    ├── __init__.py
    ├── main.py
    ├── api/
    │   ├── __init__.py
    │   └── health.py
    └── resources/
        └── __init__.py
```

### 3）把 `config/apollo_example/*` 复制到 Apollo（按 README 指引）

打开：

- `config/apollo_example/README.md`

它会明确告诉你要在 Apollo 里配置哪些 key（至少包含 `system.config` 与 `llm.models`）。

> 重要：Apollo 里这些值**通常要求是字符串**（JSON 字符串或 YAML 文本），FastChain 会在加载时做类型转换（无需你手工 JSON 解析）。

### 4）启动服务

```
uvicorn test_fastchain.main:app --host 0.0.0.0 --port 8000 --reload
```

验证：

- `GET http://localhost:8000/health` → `{"status":"ok"}`

------

## `fastchain init` 使用说明

### 默认行为

- 默认 **就地初始化**（在工程根目录执行即可）
- 默认 **不覆盖已存在文件**（除非 `--force`）
- 默认生成 `config/apollo_example/`（这是示例，不是运行时目录）
- 默认 **入口模式为 object**：生成 `<app_pkg>/main.py` 暴露 `app` 对象

最常用命令：

```
fastchain init --apollo-url "http://apollo:8080"
```

### 常用参数

- `--force`：覆盖已存在文件（慎用）
- `--dry-run`：只打印计划，不落盘
- `--app-package <name>`：业务包名（默认遵循我的习惯 demo/demo 这种同名的目录名方式：就地初始化时为“目录名”）
- `--entry-mode object|factory`
  - `object`（默认）：`uvicorn <pkg>.main:app ...`
  - `factory`：`uvicorn <pkg>.main:app --factory ...`
- `--resources-profile minimal|core`
  - `minimal`（默认不炸）：启用 `relational + llm + 项目自身资源包`
  - `core`（三件套）：启用 `relational + llm + mongo + redis`（需要补齐参数）
- `--router-module xxx`：追加 `routers.modules`（可多次）
- `--prompt-pack none|minimal|all`：prompts 样板生成策略

------

## 配置体系（必须理解的 3 件事）

### 1）本地配置：`config/settings.toml`

用于“连接 Apollo + 日志基础设置”，典型包含：

- `apollo_server_url / apollo_app_id / apollo_cluster / apollo_namespace`
- loguru 日志：console/file/graylog、enqueue、uvicorn 接管等

建议配合环境变量：

- `RUN_ENV_TYPE=dev|test|prod`
- `PROJECT_ROOT`：显式指定项目根目录（否则会向上找 `.git/pyproject.toml`）

### 2）Apollo 必备 key：`system.config`

FastChain 启动时强依赖以下结构：

- `system.config.resources.modules`：**非空 list**
- `system.config.routers.modules`：**非空 list**

并且 **LLM 必须启用**（否则 wiring 阶段会缺 `ResourceName.LLM`）。

推荐“最小可跑” resources/modules 组合（示意）：

```json
{
  "resources": {
    "modules": [
      "fastchain.core.db.relational",
      "fastchain.core.llm",
      "test_fastchain.resources"
    ]
  },
  "routers": {
    "modules": ["test_fastchain.api"]
  }
}
```

### 3）Apollo 常用 key

- `llm.models`（JSON 字符串）
  - 多模型配置 dict：key 是 alias（例如 `ds`）
- `prompts.*`（YAML 文本）
  - PromptManager 默认扫描 `prompts.` 前缀（也可在 `system.config.prompts.prefix` 修改）
- `jobs.config`（JSON 字符串）
  - Job 覆盖/开关等配置（按需）

------

## 扩展能力一：新增你自己的 Resource

除了 FastChain 内置的 relational / MongoDB / Redis / LLM / Scheduler 等资源，你也可以**编写并接入自己的资源**（例如：ES、Kafka、对象存储、第三方鉴权、内部网关客户端等）。

### FastChain 的 Resource 约定

FastChain 会在 `system.config.resources.modules` 指定的模块树中递归扫描，自动发现所有继承自 `Resource` 的类，并自动实例化注册。

约定要点：

1. 资源类必须继承 `fastchain.core.resources.base.Resource`
2. 构造函数签名必须是：

```python
def __init__(self, settings, event_bus):
    ...
```

1. 必须实现三个异步方法：

- `start()`
- `stop()`
- `health_check()`

### 最小示例：自定义一个 HTTP Gateway 资源

新建：`test_fastchain/resources/gateway.py`

```python
from __future__ import annotations

from typing import Any

import httpx
from fastchain.core.resources.base import Resource, HealthStatus
from fastchain.core.config.constants import ResourceKind


class GatewayClientResource(Resource):
    """
    示例：一个内部网关 HTTP 客户端资源

    你可以把各种外部依赖（ES/Kafka/对象存储/内部 SDK）都封装成 Resource，
    由 FastChain 统一管理生命周期，并在启动时自动装配。
    """

    def __init__(self, settings, event_bus):
        super().__init__(name="gateway_client", kind=ResourceKind.INFRA, priority=50)
        self.settings = settings
        self.event_bus = event_bus
        self.client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        # 举例：从本地配置或 Apollo 读取配置
        timeout = 10.0
        self.client = httpx.AsyncClient(timeout=timeout)

    async def stop(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    async def health_check(self):
        # 你也可以做真实探活；这里给出一个最小实现
        return HealthStatus.HEALTHY
```

### 如何让它生效？

只要你的 `system.config.resources.modules` 里包含：

- `test_fastchain.resources`

FastChain 就会扫描 `test_fastchain/resources/` 下的资源类并自动注册。

> 注意：如果你把 `test_fastchain.resources` 从 modules 里移除，你的自定义资源也不会被发现。

### 如何在代码里使用这个资源？

FastChain 的资源统一注册在 ResourceManager 中，按 name 获取（示意）：

```python
# 伪代码：在路由/服务里拿资源
rm = request.app.state.resource_manager
gateway = rm.get_resource("gateway_client")
```

（推荐：把资源注入到你自己的 Service，再由 Service 暴露更干净的调用接口。）

------

## 扩展能力二：新增你自己的 Router

FastChain 会在 `system.config.routers.modules` 指定的模块树中递归扫描，自动发现所有导出 `router` 变量的模块，并注册到 FastAPI。

新建：`test_fastchain/api/chat.py`

```python
from fastapi import APIRouter

router = APIRouter(tags=["chat"])

@router.post("/chat")
async def chat(body: dict) -> dict:
    return {"echo": body.get("query", "")}
```

确保 Apollo 的 `system.config.routers.modules` 包含 `test_fastchain.api`（init 默认就是这样）。

------

## 扩展能力三：任务调度能力（Job / Scheduler）

FastChain 支持定时任务（基于 APScheduler），并且支持：

- 从 Apollo 动态加载/热重载任务配置
- 分布式锁（Redis 可用时）避免多实例重复执行
- Redis 不可用时可优雅降级（视实现策略）

> **提醒：虽然 FastChain 提供了异步并发的后台任务能力，但是不建议在 FastChain 中部署过重的后台任务**

### 1）启用 Scheduler 资源模块

在 Apollo 的 `system.config.resources.modules` 中加入：

- `fastchain.core.scheduler`

示意：

```json
{
  "resources": {
    "modules": [
      "fastchain.core.db.relational",
      "fastchain.core.llm",
      "fastchain.core.scheduler",
      "test_fastchain.resources"
    ]
  },
  "routers": {
    "modules": ["test_fastchain.api"]
  },
  "scheduler": {
    "enabled": true,
    "timezone": "Asia/Shanghai",
    "modules": ["test_fastchain.jobs.sample"],
    "jobs": {
      "demo_heartbeat": { "enabled": true }
    }
  }
}
```

> 说明：
>
> - `scheduler.modules` 是“包含 Job 定义的 Python 模块路径列表”，SchedulerManager 会导入这些模块并注册任务。
> - 如果你需要分布式锁，请在 resources.modules 中启用 Redis 资源模块并补齐 redis 配置。

### 2）定义一个 Job（代码层）

新建：`test_fastchain/jobs/sample.py`

```python
from __future__ import annotations

from fastchain.core.scheduler.decorators import scheduled


@scheduled(
    key="demo_heartbeat",
    cron="*/1 * * * *",  # 每分钟
    distributed=False,    # 示例先关掉分布式
    enabled=True,
)
def demo_heartbeat(settings):
    # settings 是框架注入的配置门面（可访问最新配置）
    print("heartbeat ok")
```

### 3）（可选）用 Apollo 的 `jobs.config` 覆盖任务配置

`jobs.config` 作为 JSON 字符串，可以用于覆盖/开关任务（具体结构以内部约定为准；init 会生成一个样板）。
