from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple, List

# ``startup_cli`` only needs a tiny subset of Rich's API. To keep the
# startup sequence light and to avoid hard dependency on the ``rich``
# package during tests or minimal installations, we attempt to import the
# real objects but provide small fallbacks when Rich is unavailable.
try:  # pragma: no cover - exercised in tests that stub Rich
    from rich.panel import Panel
    from rich.console import Console
except Exception:  # pragma: no cover
    class Console:  # type: ignore
        """Minimal stub matching :class:`rich.console.Console`."""

        def print(self, *args, **kwargs) -> None:
            print(*args, **kwargs)

    class Panel:  # type: ignore
        """Fallback panel with ``fit`` passthrough."""

        @staticmethod
        def fit(msg: str) -> str:
            return msg

console = Console()


def render_banner() -> None:
    """Render the startup banner when running in a TTY."""
    if os.getenv("STARTUP_QUIET", "").lower() in {"1", "true", "yes", "on"}:
        return
    if sys.stdout.isatty():
        console.print(Panel.fit("[bold cyan]SolHunter Zero Startup[/]"), justify="center")


def parse_args(argv: List[str] | None = None) -> Tuple[argparse.Namespace, List[str]]:
    """Parse command line arguments for startup."""
    parser = argparse.ArgumentParser(description="Guided setup and launch")
    parser.add_argument("--skip-deps", action="store_true", help="Skip dependency check")
    parser.add_argument("--full-deps", action="store_true", help="Install optional dependencies")
    parser.add_argument("--skip-setup", action="store_true", help="Skip config and wallet prompts")
    parser.add_argument(
        "--skip-rpc-check",
        action="store_true",
        help="Skip Solana RPC availability check",
    )
    parser.add_argument(
        "--skip-endpoint-check",
        action="store_true",
        help="Skip HTTP endpoint availability check",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip internet and RPC connectivity checks",
    )
    parser.add_argument("--skip-preflight", action="store_true", help="Skip environment preflight checks")
    parser.add_argument("--self-test", action="store_true", help="Run bootstrap and preflight checks then exit")
    parser.add_argument("--one-click", action="store_true", help="Enable fully automated non-interactive startup")
    parser.add_argument(
        "--allow-rosetta",
        action="store_true",
        help="Allow running under Rosetta (no Metal acceleration)",
    )
    parser.add_argument("--diagnostics", action="store_true", help="Print system diagnostics and exit")
    parser.add_argument("--no-diagnostics", action="store_true", help="Suppress post-run diagnostics collection")
    parser.add_argument("--repair", action="store_true", help="Force macOS setup and clear dependency caches")
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Skip prompts and launch start_all.py directly",
    )
    parser.add_argument("--quiet", action="store_true", help="Reduce startup output noise")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument("--keypair", help="Path to Solana keypair file")
    parser.set_defaults(full_deps=True, one_click=True)
    args, rest = parser.parse_known_args(argv)

    if args.config:
        os.environ["SOLHUNTER_CONFIG"] = str(Path(args.config))
    if args.keypair:
        os.environ["KEYPAIR_PATH"] = str(Path(args.keypair))
    if args.quiet:
        os.environ.setdefault("STARTUP_QUIET", "1")
    return args, rest
