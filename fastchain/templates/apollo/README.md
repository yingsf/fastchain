# Apollo 配置导入指引（fastchain init 生成）

> 本目录下的文件是“可复制到 Apollo 的样板”，均为占位符/脱敏内容。
> 你需要将对应文件内容粘贴到 Apollo 配置中心的 Key-Value 中。

## 必需（启动强依赖）
- Key: `system.config`
- Value: `system.config.json` 文件内容（JSON 字符串）
- Namespace: `{apollo_namespace}`
- AppId: `{apollo_app_id}`

⚠️ 注意：
1. `resources.modules` 和 `routers.modules` 必须是**非空 list**，否则 FastChain 启动会 fail-fast
2. 如果你旧配置包含 `fastchain.api.demo.router`：本发行版已删除 demo 路由模块，需从 routers.modules 移除该条，否则启动 import 失败

## 强烈建议
- Key: `llm.models`
- Value: `llm.models.json` 文件内容（JSON 字符串）

- Key: `jobs.config`
- Value: `jobs.config.json` 文件内容（JSON 字符串）

## Prompts（YAML 文本）
FastChain 使用“多 Key + YAML 文本”的方式管理 Prompt：
- Key: `prompts.coding_master`  Value: `prompts.coding_master.yaml` 内容
- Key: `prompts.translator`     Value: `prompts.translator.yaml` 内容
- Key: `prompts.default_extraction` Value: `prompts.default_extraction.yaml` 内容

如果你修改了 prompts 前缀，需要同步更新：
- system.config 中的 `prompts.prefix`
- Apollo 中实际的 prompts.* Key 前缀
