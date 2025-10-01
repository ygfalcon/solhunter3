#!/usr/bin/env python3
"""Launch a cluster of SolHunter Zero nodes from a config file."""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Iterable

from solhunter_zero.paths import ROOT
from solhunter_zero import env  # noqa: E402

env.load_env_file(ROOT / ".env")

import tomllib

from solhunter_zero.config import ENV_VARS  # noqa: E402

PROCS: list[subprocess.Popen] = []


def _env_from_config(cfg: dict) -> dict[str, str]:
    env = {}
    for key, env_var in ENV_VARS.items():
        if key in cfg:
            env[env_var] = str(cfg[key])
    return env


def assemble_commands(cfg: dict) -> list[tuple[list[str], dict[str, str]]]:
    """Return command and env pairs for all nodes defined in ``cfg``."""
    nodes = cfg.get("nodes") or []
    common_env = _env_from_config(cfg)
    cmds: list[tuple[list[str], dict[str, str]]] = []
    for node in nodes:
        env = dict(common_env)
        env.update(_env_from_config(node))
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "start_all.py"),
        ]
        cmds.append((cmd, env))
    return cmds


def _start_processes(cmds: Iterable[tuple[list[str], dict[str, str]]]) -> None:
    for cmd, env in cmds:
        proc = subprocess.Popen(cmd, env={**os.environ, **env})
        PROCS.append(proc)


def _stop_all(*_: object) -> None:
    for p in PROCS:
        if p.poll() is None:
            p.terminate()
    for p in PROCS:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()


def load_cluster_config(path: str) -> dict:
    with open(path, "rb") as fh:
        return tomllib.load(fh)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Launch multiple nodes from a cluster configuration"
    )
    parser.add_argument(
        "config", nargs="?", default="cluster.toml", help="Cluster config file"
    )
    args = parser.parse_args(argv)

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = ROOT / cfg_path
    cfg = load_cluster_config(str(cfg_path))
    cmds = assemble_commands(cfg)

    signal.signal(signal.SIGINT, _stop_all)
    signal.signal(signal.SIGTERM, _stop_all)

    _start_processes(cmds)

    for p in PROCS:
        p.wait()

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
