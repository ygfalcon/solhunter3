#!/usr/bin/env python3
"""Interactive startup script for SolHunter Zero."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from solhunter_zero import startup_cli


def _startup_checks():
    from solhunter_zero import startup_checks

    return startup_checks


# Re-export helpers for tests and external users
def ensure_target(*args, **kwargs):
    return _startup_checks().ensure_target(*args, **kwargs)


def ensure_wallet_cli(*args, **kwargs):
    return _startup_checks().ensure_wallet_cli(*args, **kwargs)


def run_quick_setup(*args, **kwargs):
    return _startup_checks().run_quick_setup(*args, **kwargs)


def ensure_cargo(*args, **kwargs):
    return _startup_checks().ensure_cargo(*args, **kwargs)


def _main_impl(argv: list[str] | None = None) -> int:
    from solhunter_zero import startup_runner, device  # noqa: F401
    from solhunter_zero.logging_utils import log_startup, rotate_preflight_log
    from solhunter_zero.config import apply_env_overrides, load_config, find_config_file
    from solhunter_zero.bootstrap_utils import ensure_deps, ensure_endpoints
    from solhunter_zero.rpc_utils import ensure_rpc
    from scripts import preflight  # noqa: F401
    from scripts import deps  # noqa: F401
    import solhunter_zero.bootstrap_utils as bootstrap_utils  # noqa: F401

    startup_checks = _startup_checks()

    raw_args = list(sys.argv[1:] if argv is None else argv)
    if "--quiet" in raw_args:
        os.environ.setdefault("STARTUP_QUIET", "1")

    log_startup("startup launched")
    rotate_preflight_log()

    startup_cli.render_banner()
    args, rest = startup_cli.parse_args(raw_args)
    if getattr(args, "offline", False):
        os.environ["SOLHUNTER_OFFLINE"] = "1"
    if getattr(args, "quiet", False):
        os.environ.setdefault("RUST_LOG", "warn")
        os.environ.setdefault("CARGO_TERM_COLOR", "never")
        import logging

        logging.basicConfig(level=logging.WARNING)
    if args.non_interactive:
        return startup_runner.launch_only(
            rest,
            offline=getattr(args, "offline", False),
            post_launch_checks=getattr(args, "post_launch_checks", False),
            args=args,
        )
    ctx = startup_checks.perform_checks(
        args,
        rest,
        ensure_deps=ensure_deps,
        ensure_target=ensure_target,
        ensure_wallet_cli=ensure_wallet_cli,
        ensure_rpc=ensure_rpc,
        ensure_endpoints=ensure_endpoints,
        ensure_cargo=ensure_cargo,
        run_quick_setup=run_quick_setup,
        log_startup=log_startup,
        apply_env_overrides=apply_env_overrides,
        load_config=load_config,
    )
    code = ctx.pop("code", 0)
    if code:
        return code
    if ctx.get("config_path") is None:
        cfg = find_config_file() or "config.toml"
        ctx["config_path"] = Path(cfg)
    return startup_runner.run(args, ctx, log_startup=log_startup)


def main(argv: list[str] | None = None) -> int:
    return _main_impl(argv)


def run(argv: list[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    try:
        code = main(args_list)
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
    except Exception:
        if "--no-diagnostics" not in args_list:
            from scripts import diagnostics

            diagnostics.main()
        raise
    if code and "--no-diagnostics" not in args_list:
        from scripts import diagnostics

        diagnostics.main()
    return code or 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(run())
