# solhunter_zero/startup_runner.py
from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

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


def launch_only(rest: List[str], *, subprocess_module=subprocess) -> int:
    """Launch start_all.py directly."""
    env = os.environ.copy()
    if "--offline" in rest:
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

    summary_rows = ctx.get("summary_rows", [])
    if summary_rows:
        table = Table(title="Startup Summary")
        table.add_column("Item", style="cyan", no_wrap=True)
        table.add_column("Status", style="green")
        for item, status in summary_rows:
            table.add_row(item, status)
            log_startup(f"{item}: {status}")
        console.print(table)

    # Initialize AgentManager before launching services
    from solhunter_zero.agent_manager import AgentManager

    try:
        if AgentManager.from_config(config) is None:
            log_startup("AgentManager.from_config returned None")
            print("AgentManager.from_config returned None")
            return 1
    except Exception as exc:
        log_startup(f"AgentManager initialization failed: {exc}")
        print(f"Failed to initialize AgentManager: {exc}")
        return 1

    # Arguments to start_all
    if "rest" in ctx:
        rest = list(ctx["rest"] or [])
    else:
        rest = list(getattr(args, "_startup_rest", []))
    if args.non_interactive:
        return launch_only(rest, subprocess_module=subprocess_module)

    rest = [*rest, "--foreground"]

    # Run the service supervisor inline so we can exit with its code
    from scripts import start_all as start_all_module

    try:
        code = start_all_module.main(rest)
    except KeyboardInterrupt:
        code = 130

    if code == 0:
        msg = "SolHunter Zero launch complete – system ready."
        print(msg)
        log_startup(msg)
    else:
        msg = f"SolHunter Zero startup failed with exit code {code}"
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

    log_path = STARTUP_LOG
    print("Log summary:")
    print(f"  Detailed logs: {log_path}")
    log_startup(f"Log summary: see {log_path}")

    return code or hc_code
