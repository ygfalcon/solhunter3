# solhunter_zero/startup_runner.py
from __future__ import annotations

import asyncio
import contextlib
import io
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit

from rich.console import Console
from rich.table import Table

from scripts import preflight  # noqa: E402
from solhunter_zero.logging_utils import log_startup, STARTUP_LOG

__all__ = ["run", "launch_only"]

console = Console()


def log_startup_info(
    *,
    config_path: Path | None = None,
    keypair_path: Path | None = None,
    mnemonic_path: Path | None = None,
    active_keypair: str | None = None,
) -> None:
    """Append startup details to startup.log."""
    lines: List[str] = []
    if config_path:
        lines.append(f"Config path: {config_path}")
    if keypair_path:
        lines.append(f"Keypair path: {keypair_path}")
    if mnemonic_path:
        lines.append(f"Mnemonic path: {mnemonic_path}")
    if active_keypair:
        lines.append(f"Active keypair: {active_keypair}")
    for line in lines:
        log_startup(line)


def _extract_ui_targets() -> tuple[dict[str, str], str | None]:
    """Return configured UI targets along with any discovery error."""

    from solhunter_zero.production.connectivity import ConnectivityChecker

    try:
        checker = ConnectivityChecker()
    except Exception as exc:  # pragma: no cover - defensive
        return {}, f"UI target discovery failed: {exc}"

    ui_targets = [t for t in checker.targets if t.get("name") in {"ui-http", "ui-ws"}]
    urls = {t.get("name", "unknown"): str(t.get("url", "")) for t in ui_targets if t.get("name")}
    return urls, None


def _poll_ui_readiness(
    *, timeout: float = 12.0, interval: float = 0.5
) -> Tuple[bool, str, dict[str, str], list[Any]]:
    """Poll UI HTTP/WS endpoints until they respond or ``timeout`` elapses."""

    from solhunter_zero.production.connectivity import ConnectivityChecker

    try:
        checker = ConnectivityChecker()
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"UI readiness probing failed: {exc}", {}, []

    ui_targets = [t for t in checker.targets if t.get("name") in {"ui-http", "ui-ws"}]
    target_urls = {t.get("name", "unknown"): str(t.get("url", "")) for t in ui_targets if t.get("name")}
    if not ui_targets:
        return True, "UI readiness skipped (no UI endpoints configured)", target_urls, []

    checker.targets = ui_targets
    deadline = time.monotonic() + timeout
    last_ok: bool = False
    last_summary = "no ui readiness results"
    last_results: list[Any] = []

    while True:
        try:
            results = asyncio.run(checker.check_all())
        except Exception as exc:  # pragma: no cover - defensive
            return False, f"UI readiness probing failed: {exc}", target_urls, []

        summaries: List[str] = []
        healthy = True
        for result in results:
            status = "OK" if result.ok else f"FAIL ({result.error or result.status or 'unavailable'})"
            summaries.append(f"{result.name}: {status}")
            healthy = healthy and result.ok
        last_ok = healthy
        last_summary = "; ".join(summaries) if summaries else "no ui targets"
        last_results = results
        if healthy or time.monotonic() >= deadline:
            break
        time.sleep(interval)

    return last_ok, last_summary, target_urls, last_results


def _env_offline_enabled(env: dict[str, str]) -> bool:
    value = env.get("SOLHUNTER_OFFLINE", "")
    return value.lower() in {"1", "true", "yes", "on"}


def _extract_port(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
    except Exception:  # pragma: no cover - defensive
        return None
    if parsed.port:
        return str(parsed.port)
    if parsed.scheme in {"https", "wss"}:
        return "443"
    if parsed.scheme in {"http", "ws"}:
        return "80"
    return None


def _ui_summary_rows(
    *,
    targets: dict[str, str],
    results: list[Any],
    readiness_message: str | None,
    readiness_ok: bool,
) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    result_map = {getattr(r, "name", ""): r for r in results if getattr(r, "name", None)}
    labels = {"ui-http": "UI HTTP", "ui-ws": "UI WS"}

    for key, label in labels.items():
        url = targets.get(key, "")
        port = _extract_port(url) if url else None
        result = result_map.get(key)

        if result:
            status = "OK" if getattr(result, "ok", False) else f"FAIL ({getattr(result, 'error', None) or getattr(result, 'status', None) or getattr(result, 'status_code', None) or 'unavailable'})"
        elif url:
            status = "skipped (not probed)" if not readiness_ok else "unknown"
        else:
            status = "skipped (not configured)"

        details: list[str] = []
        if url:
            details.append(url)
        if port:
            details.append(f"port {port}")

        suffix = f" – {'; '.join(details)}" if details else ""
        rows.append((label, f"{status}{suffix}"))

    if not rows and readiness_message:
        rows.append(("UI readiness", readiness_message))

    return rows


def launch_only(
    rest: List[str], *, offline: bool | None = None, subprocess_module=subprocess
) -> int:
    """Launch start_all.py directly."""
    env = os.environ.copy()
    offline_enabled = offline if offline is not None else _env_offline_enabled(env)
    if offline_enabled or "--offline" in rest:
        env["SOLHUNTER_OFFLINE"] = "1"
    proc = subprocess_module.run([sys.executable, "scripts/start_all.py", *rest], env=env)
    return proc.returncode


def run(
    args,
    ctx: Dict[str, Any],
    *,
    log_startup=log_startup,
    subprocess_module=subprocess,
) -> int:
    """Render summary table and launch start_all."""
    config_path = ctx.get("config_path")
    config = ctx.get("config")

    # Ensure config is loaded
    if not config_path:
        from solhunter_zero.config import find_config_file, load_config

        config_path = Path(find_config_file() or "config.toml")
        ctx["config_path"] = config_path
        if config is None:
            config = load_config(config_path)
            ctx["config"] = config
    elif config is None:
        from solhunter_zero.config import load_config

        config = load_config(config_path)
        ctx["config"] = config

    # Log useful paths
    log_startup_info(
        config_path=config_path,
        keypair_path=ctx.get("keypair_path"),
        mnemonic_path=ctx.get("mnemonic_path"),
        active_keypair=ctx.get("active_keypair"),
    )

    # Bubble BirdEye key into env so token discovery has it
    birdeye = (config or {}).get("birdeye_api_key")
    if birdeye and not os.getenv("BIRDEYE_API_KEY"):
        os.environ["BIRDEYE_API_KEY"] = str(birdeye)
        try:
            from solhunter_zero import scanner_common

            scanner_common.BIRDEYE_API_KEY = os.environ["BIRDEYE_API_KEY"]
            scanner_common.HEADERS["X-API-KEY"] = scanner_common.BIRDEYE_API_KEY
        except Exception:
            pass

    summary_rows = list(ctx.get("summary_rows", []))

    # Initialize AgentManager before launching services
    from solhunter_zero.agent_manager import AgentManager

    agent_manager_error: Exception | None = None
    agent_manager_tb: str | None = None
    try:
        if AgentManager.from_config(config) is None:
            agent_manager_error = RuntimeError(
                "AgentManager.from_config returned None"
            )
    except Exception as exc:
        agent_manager_error = exc
        agent_manager_tb = traceback.format_exc()

    if agent_manager_error:
        guidance = (
            f"AgentManager initialization failed: {agent_manager_error}. "
            "Verify [agent_manager] and [agents] config sections are present and valid."
        )
        log_startup(guidance)
        if agent_manager_tb:
            log_startup(agent_manager_tb)
        print(guidance)
        summary_rows.append(("AgentManager", f"failed – {agent_manager_error}"))
        code = 1
    else:
        code: int | None = None

    # Arguments to start_all
    rest = list(ctx.get("rest", []))
    if code is None:
        if args.non_interactive:
            return launch_only(rest, offline=getattr(args, "offline", False), subprocess_module=subprocess_module)

        rest = [*rest, "--foreground"]

        # Run the service supervisor inline so we can exit with its code
        from scripts import start_all as start_all_module

        try:
            code = start_all_module.main(rest)
        except KeyboardInterrupt:
            code = 130

    ui_ready: tuple[bool, str, dict[str, str], list[Any]] | None = None
    ui_targets: dict[str, str] = {}
    ui_results: list[Any] = []
    ui_message: str | None = None
    if code == 0:
        ui_ready = _poll_ui_readiness()
        ui_targets = ui_ready[2]
        ui_results = ui_ready[3]
        status_label = "healthy" if ui_ready[0] else "unhealthy"
        ui_msg = f"UI readiness {status_label}: {ui_ready[1]}"
        ui_message = ui_msg
        print(ui_msg)
        log_startup(ui_msg)
        msg = "SolHunter launch complete – system ready."
        print(msg)
        log_startup(msg)
    else:
        ui_targets, ui_error = _extract_ui_targets()
        if ui_error:
            ui_message = ui_error
            log_startup(ui_error)
        status_label = "skipped (startup failed)"
        ui_msg = f"UI readiness {status_label}"
        ui_message = ui_message or ui_msg
        print(ui_msg)
        log_startup(ui_msg)
        msg = f"SolHunter startup failed with exit code {code}"
        print(msg)
        log_startup(msg)

    # Healthcheck summary (post-launch)
    from scripts import healthcheck

    selected = list(preflight.CHECKS)
    critical = {name for name, _ in selected}
    non_critical = {"Homebrew", "Rustup", "Rust", "Xcode CLT", "GPU"}
    critical -= non_critical
    if args.skip_deps:
        selected = [c for c in selected if c[0] != "Dependencies"]
        critical.discard("Dependencies")
    if args.skip_setup:
        selected = [c for c in selected if c[0] not in {"Config", "Keypair"}]
        critical.difference_update({"Config", "Keypair"})
    if args.skip_rpc_check or args.offline:
        selected = [c for c in selected if c[0] != "Network"]
        critical.discard("Network")
    if args.skip_preflight:
        selected = []
        critical = set()

    hc_out = io.StringIO()
    hc_err = io.StringIO()
    with contextlib.redirect_stdout(hc_out), contextlib.redirect_stderr(hc_err):
        try:
            hc_code = healthcheck.main(selected, critical=critical)
        except SystemExit as exc:  # defensive
            hc_code = exc.code if isinstance(exc.code, int) else 1

    out = hc_out.getvalue()
    err = hc_err.getvalue()
    sys.stdout.write(out)
    sys.stderr.write(err)
    for line in (out + err).splitlines():
        if line:
            log_startup(line)

    summary_rows.extend(
        _ui_summary_rows(
            targets=ui_targets,
            results=ui_results,
            readiness_message=ui_message,
            readiness_ok=ui_ready[0] if ui_ready else False,
        )
    )
    if summary_rows:
        table = Table(title="Startup Summary")
        table.add_column("Item", style="cyan", no_wrap=True)
        table.add_column("Status", style="green")
        for item, status in summary_rows:
            table.add_row(item, status)
            log_startup(f"{item}: {status}")
        console.print(table)

    log_path = STARTUP_LOG
    print("Log summary:")
    print(f"  Detailed logs: {log_path}")
    log_startup(f"Log summary: see {log_path}")

    return code or hc_code
