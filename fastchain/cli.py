from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable, Literal

from .core.server.bootstrap import get_app_version

try:
    from importlib import resources as importlib_resources  # py3.9+
except ImportError:  # pragma: no cover
    import importlib_resources  # type: ignore


# -----------------------------
# 模板辅助
# -----------------------------
def _read_template(relpath: str) -> str:
    """读取随包分发的模板文件内容"""
    pkg = "fastchain.templates"
    return importlib_resources.files(pkg).joinpath(relpath).read_text(encoding="utf-8")


def _render_settings_toml(
        *,
        apollo_server_url: str,
        apollo_app_id: str,
        apollo_cluster: str,
        apollo_namespace: str,
) -> str:
    """渲染工程的 config/settings.toml

    注意：本文件是本地静态配置（用于连接 Apollo / 日志基础设置）
    业务运行期的核心配置仍以 Apollo 为准（system.config / llm.models / jobs.config 等）
    """

    # 使用 JSON 字符串字面量来安全转义（同时兼容 TOML 的基本字符串写法）
    def q(v: str) -> str:
        return json.dumps(v, ensure_ascii=False)

    return (
        "# 默认环境（default）：所有环境共享的基础配置\n"
        "[default]\n"
        f"apollo_server_url = {q(apollo_server_url)}\n"
        f"apollo_app_id = {q(apollo_app_id)}\n"
        f"apollo_cluster = {q(apollo_cluster)}\n"
        f"apollo_namespace = {q(apollo_namespace)}\n\n"
        "# ---------------------- 日志配置（建议保留） ----------------------\n"
        "[logging]\n"
        'level = "INFO"\n'
        "enqueue = true\n"
        "backtrace = false\n"
        "diagnose = false\n"
        "enable_uvicorn = true\n"
        'sinks = ["console", "file"]\n'
        'format_str = """[<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>] - <level>{message}</level>"""\n\n'
        "[logging.console]\n"
        "enabled = true\n\n"
        "[logging.file]\n"
        "enabled = true\n"
        'rotation = "20 MB"\n'
        'compression = "zip"\n\n'
        "[logging.graylog]\n"
        "enabled = false\n"
        'host = "127.0.0.1"\n'
        "port = 12201\n"
        'protocol = "udp"\n'
        'source = "fastchain-dev"\n\n'
        "# ---------------------- 环境覆盖示例（可选） ----------------------\n"
        "# 如果你在不同环境使用不同的 Apollo 地址，可启用下面的覆盖块：\n"
        "# [dev]\n"
        '# apollo_server_url = "http://apollo-dev.internal:8080"\n\n'
        "# [prod]\n"
        '# apollo_server_url = "http://apollo-prod.internal:8080"\n'
    )


def _ensure_dir(path: Path, *, dry_run: bool = False) -> None:
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)


def _write_file(path: Path, content: str, *, force: bool, dry_run: bool = False) -> None:
    """写入文件；默认不覆盖，除非 force=True"""
    if path.exists() and not force:
        raise FileExistsError(f"文件已存在：{path}（如需覆盖请添加 --force）")
    if dry_run:
        return
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, obj: object, *, force: bool, dry_run: bool = False) -> None:
    """写入 JSON 文件（indent=4，UTF-8，无 ASCII 转义）"""
    content = json.dumps(obj, ensure_ascii=False, indent=4, sort_keys=False) + "\n"
    _write_file(path, content, force=force, dry_run=dry_run)


def _print_plan(items: Iterable[str]) -> None:
    for it in items:
        print(it)


# -----------------------------
# Apollo system.config 构建
# -----------------------------
ResourcesProfile = Literal["minimal", "core"]
PromptPack = Literal["none", "minimal", "all"]
EntryMode = Literal["object", "factory"]


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _build_system_config(
        *,
        profile: ResourcesProfile,
        resource_modules: list[str],
        router_modules: list[str],
        db_url: str,
        mongo_uri: str | None,
        mongo_database: str | None,
        mongo_models_package: str | None,
        redis_url: str | None,
) -> dict:
    """构建 Apollo 中 system.config 的配置对象（最终写入为 JSON 字符串）

    约束（来自 fastchain 启动链路的强校验）：
    - system.config 必须存在
    - value 必须是 JSON 字符串（json.loads 后为 dict）
    - 且必须包含：
        - resources.modules：非空 list[str]
        - routers.modules：非空 list[str]
    """
    cfg: dict = {
        "apollo": {
            "poll_interval": 10,
            "qps": 5,
            "max_concurrency": 10,
        },
        "database": {
            "url": db_url,
            "echo": False,
            "pool_size": 20,
            "max_overflow": 10,
            "pool_recycle": 3600,
        },
        "prompts": {"prefix": "prompts."},
        "llm_service": {
            "structure_retry_count": 3,
            "connection_limits": {
                "max_keepalive_connections": 20,
                "max_connections": 100,
            },
        },
        "security": {
            "parser_max_length": 1_000_000,
            "audit_log_max_length": 2000,
            "stream_buffer_max_tokens": 10_000,
            "cors": {
                "enabled": False,
                "allow_origins": ["http://localhost:3000"],
                "allow_methods": ["*"],
                "allow_headers": ["*"],
                "allow_credentials": True,
                "max_age": 600,
            },
        },
        "timeouts": {
            "watcher_tick_timeout": 60.0,
            "watcher_stop_timeout": 5.0,
            "redis_health_interval": 30.0,
            "mongo_health_interval": 60.0,
            "db_health_interval": 60.0,
        },
        "scheduler": {
            "enabled": False,
            "timezone": "Asia/Shanghai",
            "lock_ttl_ms": 60_000,
            "modules": [],
            "jobs": {},
        },
        "resources": {"modules": resource_modules},
        "routers": {"modules": router_modules},
    }

    if profile == "core":
        cfg["mongodb"] = {
            "uri": mongo_uri,
            "database": mongo_database,
            "models_package": mongo_models_package,
            "model_discovery_concurrency": 20,
            "min_pool_size": 10,
            "max_pool_size": 100,
            "timeout_ms": 5000,
        }
        cfg["redis"] = {
            "url": redis_url,
            "max_connections": 20,
            "socket_timeout": 5.0,
            "socket_connect_timeout": 2.0,
            "retry_on_timeout": True,
            "encoding": "utf-8",
            "decode_responses": True,
        }
    else:
        # minimal：仅占位，不启用模块不会生效
        cfg["mongodb"] = {
            "uri": "mongodb://<user>:<password>@<host>:<port>/?authSource=<db>&authMechanism=SCRAM-SHA-1",
            "database": "<db_name>",
            "models_package": "<your.models.package>",
            "model_discovery_concurrency": 20,
            "min_pool_size": 10,
            "max_pool_size": 100,
            "timeout_ms": 5000,
        }
        cfg["redis"] = {
            "url": "redis://:<password>@<host>:<port>/0",
            "max_connections": 20,
            "socket_timeout": 5.0,
            "socket_connect_timeout": 2.0,
            "retry_on_timeout": True,
            "encoding": "utf-8",
            "decode_responses": True,
        }

    return cfg


# -----------------------------
# 防呆
# -----------------------------
def _looks_like_project_root(path: Path) -> bool:
    """判断目录是否看起来已经是工程根

    只要满足任意条件，就认为是工程根：
    - 存在 pyproject.toml
    - 存在 uv.lock
    - 存在 .venv/ 目录
    """
    return (path / "pyproject.toml").exists() or (path / "uv.lock").exists() or (path / ".venv").is_dir()


def _resolve_target_path(raw_path: str) -> Path:
    """解析 init 的目标目录

    防呆兼容：
    - 在 test_fastchain/ 内执行：fastchain init test_fastchain
      旧逻辑会生成 test_fastchain/test_fastchain；现在按 '.' 就地初始化处理。
    """
    raw_path = (raw_path or ".").strip()
    cwd = Path.cwd().resolve()

    if raw_path in ("", "."):
        return cwd

    if raw_path == cwd.name:
        return cwd

    return Path(raw_path).expanduser().resolve()


def _post_init_reminder(
        *,
        apollo_namespace: str,
        apollo_app_id: str,
        resources_profile: ResourcesProfile,
        prompt_pack: PromptPack,
) -> None:
    """init 完成后输出强提醒（不做 Apollo 联网校验）"""
    lines = [
        "\n================= 必读提醒（Apollo 配置）=================",
        "【启动必需】以下 Key 名在 fastchain 代码中是硬编码约定：",
        f"1) system.config（namespace={apollo_namespace}, appId={apollo_app_id}）",
        "   - value 必须是 JSON 字符串",
        "   - 且必须包含：resources.modules / routers.modules（两者都必须是非空 list）",
        "2) llm.models",
        "   - value 必须是 JSON 字符串，解析后应为 dict",
        "   - provider_type 仅支持 openai / china_unicom",
        "",
        "【资源模块启用策略】",
        f"3) resources-profile={resources_profile}",
    ]

    if resources_profile == "minimal":
        lines += [
            "   - 默认启用 relational + llm + 项目自身资源包，保证框架可启动",
            "   - mongodb/redis 仅提供占位配置块，不启用对应资源模块则不会生效（不炸）",
            "   - 三件套（relational+mongo+redis）请用：--resources-profile core 并补齐 mongo/redis 参数",
            "",
        ]
    else:
        lines += [
            "   - 已启用 relational + llm + MongoDB + Redis（缺配置会 fail-fast，因此 CLI 强制要求提供参数）",
            "",
        ]

    lines += [
        "【按需配置】",
        "4) jobs.config",
        '   - value 必须是 JSON 字符串，结构通常为 {"schedules": {...}}',
        "",
        "【Prompts 配置约定】",
        "5) prompts.*（多 key）",
        "   - 每个 prompts.xxx 的 value 是 YAML 文本（不是 JSON）",
        "   - PromptManager 默认扫描前缀 prompts.；也可通过 system.config.prompts.prefix 指定",
        f"   - 本次 prompt-pack={prompt_pack}（prompts 不是框架启动必需，是否需要取决于业务是否引用对应 key）",
        "==========================================================\n",
    ]

    print("\n".join(lines))


# -----------------------------
# main.py 生成
# -----------------------------
def _render_pkg_main_object_mode() -> str:
    """业务包内 main.py（对象模式）：提供 app 对象，避免 --factory 误用"""
    return (
        '"""FastChain 业务包入口（对象模式）\n\n'
        "推荐启动方式：\n\n"
        "    uvicorn <your_package>.main:app --host 0.0.0.0 --port 8000 --reload\n\n"
        "说明：本文件在 import 时会创建 ASGI app（并触发配置加载）。\n"
        "如需延迟初始化/更复杂的启动流程，可在生成项目后自行改造。\n"
        '"""\n\n'
        "from fastchain.main import create_app as _create_app\n\n"
        "# ASGI 应用对象（符合 uvicorn xxx:app 的默认约定）\n"
        "app = _create_app()\n"
    )


def _render_pkg_main_factory_mode() -> str:
    """业务包内 main.py（工厂模式）：提供 app() 工厂函数，需要 --factory"""
    return (
        '"""FastChain 业务包入口（工厂模式）\n\n'
        "启动方式：\n\n"
        "    uvicorn <your_package>.main:app --factory --host 0.0.0.0 --port 8000 --reload\n\n"
        "说明：工厂模式可避免 import 阶段触发重型初始化。\n"
        "但对新手而言容易忘记 --factory；因此 CLI 默认使用对象模式。\n"
        '"""\n\n'
        "from fastchain.main import create_app as _create_app\n\n\n"
        "def app():\n"
        '    """ASGI 应用工厂函数（供 Uvicorn --factory 调用）"""\n'
        "    return _create_app()\n"
    )


def _render_root_run_forwarder(app_pkg: str) -> str:
    """工程根目录 run.py：转发入口（可选），避免与业务包 main.py 同名造成误解"""
    return (
        '"""FastChain 工程根入口（转发，可选）\n\n'
        "推荐使用业务包入口启动：\n\n"
        f"    uvicorn {app_pkg}.main:app --host 0.0.0.0 --port 8000 --reload\n\n"
        "本文件仅用于兼容少数人习惯：\n\n"
        "    uvicorn run:app --host 0.0.0.0 --port 8000 --reload\n"
        '"""\n\n'
        f"from {app_pkg}.main import app  # noqa: F401\n"
    )


# -----------------------------
# Commands
# -----------------------------
def cmd_init(args: argparse.Namespace) -> int:
    cwd = Path.cwd().resolve()
    target = _resolve_target_path(args.path)

    cwd_is_project_root = _looks_like_project_root(cwd)

    # 防呆：如果当前目录已是工程根，但用户又要在子目录生成“新项目”，默认拒绝。
    if cwd_is_project_root and target != cwd and not args.allow_nested:
        raise SystemExit(
            "检测到当前目录已包含工程标识（pyproject.toml/uv.lock/.venv）。\n"
            "为避免误生成“项目套项目”，默认禁止在子目录创建新项目。\n"
            "如果你确实要这么做，请显式添加参数：--allow-nested"
        )

    # 工程根补齐模式：目标目录就是 cwd，且 cwd 看起来已初始化
    in_place_mode = target == cwd and _looks_like_project_root(target)

    project_name = args.project_name or target.name

    # 业务包命名：就地初始化默认使用“目录名”实现 demo/demo；新项目默认 app
    app_pkg = (args.app_package.strip() if args.app_package else (target.name if in_place_mode else "app"))

    # routers/resources 默认组合（随 app_pkg 变化）
    resource_root = f"{app_pkg}.resources"
    default_router_module = f"{app_pkg}.api"
    router_modules = _dedupe_keep_order([default_router_module] + (args.router_module or []))

    resources_profile: ResourcesProfile = args.resources_profile

    # minimal 也必须默认启用 LLM，否则启动 wiring 阶段会缺 ResourceName.LLM
    base_resources = ["fastchain.core.db.relational", "fastchain.core.llm"]
    core_resources = ["fastchain.core.db.mongo", "fastchain.core.db.redis"]

    resource_modules = (
        _dedupe_keep_order(base_resources + core_resources + [resource_root])
        if resources_profile == "core"
        else _dedupe_keep_order(base_resources + [resource_root])
    )

    # 数据源参数
    db_url = args.db_url.strip() if args.db_url else "sqlite+aiosqlite:///./fastchain.db"
    mongo_uri = args.mongo_uri.strip() if args.mongo_uri else None
    mongo_database = args.mongo_database.strip() if args.mongo_database else None
    mongo_models_package = args.mongo_models_package.strip() if args.mongo_models_package else None
    redis_url = args.redis_url.strip() if args.redis_url else None

    if resources_profile == "core":
        missing = [
            flag
            for flag, value in [
                ("--mongo-uri", mongo_uri),
                ("--mongo-database", mongo_database),
                ("--mongo-models-package", mongo_models_package),
                ("--redis-url", redis_url),
            ]
            if not value
        ]
        if missing:
            raise SystemExit(
                "resources-profile=core 需要补齐以下参数，否则新工程将因缺配置启动 fail-fast："
                + ", ".join(missing)
            )

    prompt_pack: PromptPack = args.prompt_pack
    entry_mode: EntryMode = args.entry_mode

    # 示例目录：apollo_example（不是运行时目录）
    apollo_example_dir = target / "config" / "apollo_example"

    # 是否生成 pyproject/.env：防呆策略
    # 只要 pyproject.toml 不存在且未显式禁用，就补齐
    with_pyproject = bool(args.with_pyproject) and (not (target / "pyproject.toml").exists())
    with_env = bool(args.with_env) and (not (target / ".env").exists())

    # 只生成一个 main.py：业务包内 <app_package>/main.py
    pkg_main_path = target / app_pkg / "main.py"

    # 可选：根目录生成 run.py 转发入口（默认不生成，避免歧义）
    root_run_path = target / "run.py"

    plan = [
        f"目标目录：{target}",
        f"模式：{'工程根补齐' if in_place_mode else '新项目生成'}",
        f"业务包名：{app_pkg}",
        f"入口模式：{entry_mode}（{'无需 --factory' if entry_mode == 'object' else '需要 --factory'}）",
        "将创建/补齐项目骨架：",
        f"  - {target / 'config' / 'settings.toml'}",
        f"  - {target / 'log'}",
        f"  - {target / 'data'}",
        f"  - {pkg_main_path}",
        f"  - {target / app_pkg / '__init__.py'}",
        f"  - {target / app_pkg / 'resources' / '__init__.py'}",
        f"  - {target / app_pkg / 'api' / '__init__.py'}",
        f"  - {target / app_pkg / 'api' / 'health.py'}",
    ]

    if args.with_root_entry:
        plan += [f"  - {root_run_path}（可选：工程根转发入口，不与 main.py 同名，避免歧义）"]

    if args.write_apollo_examples:
        plan += [
            "将生成 Apollo 样板（脱敏占位符，可复制到 Apollo）：",
            f"  - {apollo_example_dir / 'system.config.json'}",
            f"  - {apollo_example_dir / 'llm.models.json'}",
            f"  - {apollo_example_dir / 'jobs.config.json'}",
            f"  - {apollo_example_dir / 'README.md'}",
        ]
        if prompt_pack in ("minimal", "all"):
            plan += [f"  - {apollo_example_dir / 'prompts.default_extraction.yaml'}"]
        if prompt_pack == "all":
            plan += [
                f"  - {apollo_example_dir / 'prompts.coding_master.yaml'}",
                f"  - {apollo_example_dir / 'prompts.translator.yaml'}",
            ]

    if with_pyproject:
        plan += [f"  - {target / 'pyproject.toml'}（新项目依赖声明）"]
    else:
        if (target / "pyproject.toml").exists():
            plan += ["跳过：pyproject.toml（已存在）"]
        else:
            plan += ["跳过：pyproject.toml（已禁用：--no-pyproject）"]

    if with_env:
        plan += [f"  - {target / '.env'}"]
    else:
        if (target / ".env").exists():
            plan += ["跳过：.env（已存在）"]

    if args.dry_run:
        _print_plan(plan)
        return 0

    # 目录结构
    _ensure_dir(target)
    _ensure_dir(target / "config")
    _ensure_dir(target / "log")
    _ensure_dir(target / "data")
    _ensure_dir(target / app_pkg)
    _ensure_dir(target / app_pkg / "resources")
    _ensure_dir(target / app_pkg / "api")

    # settings.toml
    settings_content = _render_settings_toml(
        apollo_server_url=args.apollo_url.strip(),
        apollo_app_id=args.apollo_app_id.strip(),
        apollo_cluster=args.apollo_cluster.strip(),
        apollo_namespace=args.apollo_namespace.strip(),
    )
    _write_file(target / "config" / "settings.toml", settings_content, force=args.force)

    # 业务包 main.py（默认对象模式：无需 --factory）
    pkg_main_content = _render_pkg_main_object_mode() if entry_mode == "object" else _render_pkg_main_factory_mode()
    _write_file(pkg_main_path, pkg_main_content, force=args.force)

    # 可选：工程根 run.py（转发入口，不与 main.py 同名）
    if args.with_root_entry:
        _write_file(root_run_path, _render_root_run_forwarder(app_pkg), force=args.force)

    # 业务包骨架
    _write_file(target / app_pkg / "__init__.py", _read_template("project_app_init.py"), force=args.force)
    _write_file(target / app_pkg / "resources" / "__init__.py", _read_template("project_resources_init.py"), force=args.force)
    _write_file(target / app_pkg / "api" / "__init__.py", _read_template("project_api_init.py"), force=args.force)
    _write_file(target / app_pkg / "api" / "health.py", _read_template("project_api_health.py"), force=args.force)

    # Apollo 样板（明确为 apollo_example）
    if args.write_apollo_examples:
        _ensure_dir(apollo_example_dir)

        system_cfg = _build_system_config(
            profile=resources_profile,
            resource_modules=resource_modules,
            router_modules=router_modules,
            db_url=db_url,
            mongo_uri=mongo_uri,
            mongo_database=mongo_database,
            mongo_models_package=mongo_models_package,
            redis_url=redis_url,
        )
        _write_json(apollo_example_dir / "system.config.json", system_cfg, force=args.force)

        _write_file(apollo_example_dir / "llm.models.json", _read_template("apollo/llm.models.json"), force=args.force)
        _write_file(apollo_example_dir / "jobs.config.json", _read_template("apollo/jobs.config.json"), force=args.force)

        if prompt_pack in ("minimal", "all"):
            _write_file(
                apollo_example_dir / "prompts.default_extraction.yaml",
                _read_template("apollo/prompts.default_extraction.yaml"),
                force=args.force,
            )
        if prompt_pack == "all":
            _write_file(
                apollo_example_dir / "prompts.coding_master.yaml",
                _read_template("apollo/prompts.coding_master.yaml"),
                force=args.force,
            )
            _write_file(
                apollo_example_dir / "prompts.translator.yaml",
                _read_template("apollo/prompts.translator.yaml"),
                force=args.force,
            )

        readme_lines = [
            "# Apollo 配置导入指引（fastchain init 生成的示例文件）\n\n",
            "> ⚠️ 本目录名为 `apollo_example`，用于存放“可复制到 Apollo 的样板”。\n",
            "> ⚠️ 这些文件不是运行时读取目录，运行时读取的是 Apollo 配置中心。\n\n",
            "## 必需（启动强依赖）\n",
            f"- Namespace: `{args.apollo_namespace.strip()}`\n",
            f"- AppId: `{args.apollo_app_id.strip()}`\n\n",
            "- Key: `system.config`\n",
            "- Value: `system.config.json` 文件内容（JSON 字符串）\n\n",
            "- Key: `llm.models`\n",
            "- Value: `llm.models.json` 文件内容（JSON 字符串；请自行填充/替换敏感密钥）\n\n",
            "⚠️ 注意：\n",
            "1. `resources.modules` 和 `routers.modules` 必须是**非空 list**，否则 FastChain 启动会 fail-fast\n",
            "2. 如果你旧配置包含 `fastchain.api.demo.router`：本发行版已删除 demo 路由模块，需从 routers.modules 移除该条，否则启动 import 失败\n\n",
            "## 资源模块 profile 说明\n",
            f"- resources-profile = `{resources_profile}`\n",
        ]

        if resources_profile == "minimal":
            readme_lines += [
                "  - 默认启用 relational + llm + 项目资源包，保证框架可启动\n",
                "  - system.config 中包含 mongodb/redis 占位配置块，但不会生效（未启用对应资源模块）\n",
                "  - 如需启用 MongoDB/Redis，请用：`--resources-profile core` 并提供 mongo/redis 参数\n\n",
            ]
        else:
            readme_lines += [
                "  - 已启用 relational + llm + MongoDB + Redis；缺配置会 fail-fast，所以 CLI 已强制要求你提供参数\n\n",
            ]

        readme_lines += [
            "## 按需配置\n",
            "- Key: `jobs.config`\n",
            "- Value: `jobs.config.json` 文件内容（JSON 字符串）\n\n",
            "## Prompts（YAML 文本，多 Key）\n",
            "FastChain 使用“多 Key + YAML 文本”的方式管理 Prompt：\n",
        ]

        if prompt_pack == "none":
            readme_lines += ["- 本次未生成 prompts 样板（prompt-pack=none）\n"]
        else:
            readme_lines += ["- Key: `prompts.default_extraction`  Value: `prompts.default_extraction.yaml`\n"]
            if prompt_pack == "all":
                readme_lines += [
                    "- Key: `prompts.coding_master`       Value: `prompts.coding_master.yaml`\n",
                    "- Key: `prompts.translator`          Value: `prompts.translator.yaml`\n",
                ]

        _write_file(apollo_example_dir / "README.md", "".join(readme_lines), force=args.force)

    # pyproject.toml（只要不存在就补齐）
    if with_pyproject:
        pyproject_tpl = _read_template("project_pyproject.toml")
        pyproject_content = pyproject_tpl.format(
            project_name=project_name.replace(" ", "-"),
            fastchain_version=get_app_version(),
        )
        _write_file(target / "pyproject.toml", pyproject_content, force=args.force)

    # .env（仅当不存在且用户未禁用时生成）
    if with_env:
        env_tpl = _read_template("env")
        env_content = env_tpl.format(
            run_env_type=args.env,
            project_root=str(target),
        )
        _write_file(target / ".env", env_content, force=args.force)

    print(f"✅ FastChain 项目骨架已生成：{target}")
    _post_init_reminder(
        apollo_namespace=args.apollo_namespace.strip(),
        apollo_app_id=args.apollo_app_id.strip(),
        resources_profile=resources_profile,
        prompt_pack=prompt_pack,
    )

    if in_place_mode:
        print("✅ 检测到当前目录已存在工程标识（uv.lock/.venv 等）。CLI 将以“补齐缺失文件”为主，不会覆盖已有 pyproject.toml。")
        print("✅ 注意：FastChain CLI 不会创建虚拟环境；请使用 `uv venv` / `uv sync` 管理项目依赖。")

    if args.write_apollo_examples:
        print("下一步：请打开 config/apollo_example/README.md，按指引把样板配置复制到 Apollo。")

    # 推荐启动命令：业务包入口
    if entry_mode == "object":
        print(f"启动（推荐）：uvicorn {app_pkg}.main:app --host 0.0.0.0 --port 8000 --reload")
        if args.with_root_entry:
            print("启动（可选）：uvicorn run:app --host 0.0.0.0 --port 8000 --reload")
    else:
        print(f"启动（推荐）：uvicorn {app_pkg}.main:app --factory --host 0.0.0.0 --port 8000 --reload")
        if args.with_root_entry:
            print("启动（可选）：uvicorn run:app --factory --host 0.0.0.0 --port 8000 --reload")

    return 0


def cmd_version(_: argparse.Namespace) -> int:
    print(get_app_version())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fastchain", description="FastChain CLI (CUCC distribution)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="生成或补齐 FastChain 项目骨架（极致防呆：默认就地初始化）")

    # path 可选：默认 '.'
    p_init.add_argument("path", nargs="?", default=".", help="目标目录（默认当前目录）")

    # 防呆开关：默认禁止“项目套项目”
    p_init.add_argument(
        "--allow-nested",
        action="store_true",
        help="允许在已初始化工程目录下创建子项目（可能导致项目套项目；默认禁止）",
    )

    p_init.add_argument("--project-name", default=None, help="生成项目的名称（用于 pyproject.toml）")
    p_init.add_argument("--env", default=os.getenv("RUN_ENV_TYPE", "dev"), help="RUN_ENV_TYPE（默认 dev）")

    # ✅ 入口模式：默认 object（无需 --factory）
    p_init.add_argument(
        "--entry-mode",
        choices=["object", "factory"],
        default="object",
        help="入口模式：object（默认，无需 --factory）/ factory（需要 --factory）",
    )

    # ✅ 默认不生成根目录入口文件，避免同名 main.py 造成误解
    p_init.add_argument(
        "--with-root-entry",
        action="store_true",
        default=False,
        help="可选：在工程根目录生成 run.py（转发入口），便于少数人从根目录启动（默认不生成）",
    )

    # app-package：就地初始化默认使用“目录名”实现 demo/demo；新项目默认 app
    p_init.add_argument("--app-package", default=None, help="业务代码包名（默认：就地初始化=目录名；新项目=app）")

    p_init.add_argument("--force", action="store_true", help="覆盖已存在文件")
    p_init.add_argument("--dry-run", action="store_true", help="仅输出将创建的文件列表，不落盘")

    # 即使默认开启，若检测到工程已初始化，也会“补齐缺失 pyproject”，但不会覆盖已有文件
    p_init.add_argument(
        "--with-pyproject",
        action="store_true",
        default=True,
        help="生成 pyproject.toml（默认开启；若已存在则跳过）",
    )
    p_init.add_argument("--no-pyproject", action="store_false", dest="with_pyproject", help="不生成 pyproject.toml")

    p_init.add_argument("--with-env", action="store_true", default=True, help="生成 .env（默认开启；若已存在则跳过）")
    p_init.add_argument("--no-env", action="store_false", dest="with_env", help="不生成 .env")

    # Apollo samples
    p_init.add_argument("--write-apollo-examples", action="store_true", default=True, help="生成 Apollo 样板文件（默认生成）")
    p_init.add_argument("--no-apollo-examples", action="store_false", dest="write_apollo_examples", help="不生成 Apollo 样板文件")

    # Profiles & composition
    p_init.add_argument(
        "--resources-profile",
        choices=["minimal", "core"],
        default="minimal",
        help="资源模块 profile：minimal（默认不炸）/ core（启用 relational+mongo+redis，需补齐参数）",
    )
    p_init.add_argument(
        "--router-module",
        action="append",
        default=[],
        help="追加 routers.modules 条目（可重复传入）。默认始终包含 <app_package>.api。",
    )

    p_init.add_argument(
        "--prompt-pack",
        choices=["none", "minimal", "all"],
        default="minimal",
        help="生成 prompts 样板：none（不生成）/ minimal（仅 default_extraction）/ all（再加 coding_master/translator）",
    )

    # Apollo connection (required for settings.toml)
    p_init.add_argument("--apollo-url", required=True, help="Apollo server URL（必填）")
    p_init.add_argument("--apollo-app-id", default="fastchain", help="Apollo appId（默认 fastchain）")
    p_init.add_argument("--apollo-cluster", default="default", help="Apollo cluster（默认 default）")
    p_init.add_argument("--apollo-namespace", default="application", help="Apollo namespace（默认 application）")

    # DB/Mongo/Redis params (written into system.config.json sample)
    p_init.add_argument("--db-url", default="sqlite+aiosqlite:///./fastchain.db", help="relational 数据库 URL（默认 sqlite）")

    p_init.add_argument("--mongo-uri", default=None, help="MongoDB URI（core profile 必填）")
    p_init.add_argument("--mongo-database", default=None, help="MongoDB database（core profile 必填）")
    p_init.add_argument("--mongo-models-package", default=None, help="Mongo models 包路径（core profile 必填）")
    p_init.add_argument("--redis-url", default=None, help="Redis URL（core profile 必填）")

    p_init.set_defaults(func=cmd_init)

    p_ver = sub.add_parser("version", help="输出 fastchain-cucc 包版本号")
    p_ver.set_defaults(func=cmd_version)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
