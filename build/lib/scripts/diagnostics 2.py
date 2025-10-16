from __future__ import annotations

import os
import platform
import subprocess
import shutil
from pathlib import Path
from typing import Any


def _rust_version(cmd: str) -> str:
    if shutil.which(cmd) is None:
        return "not installed"
    try:
        return subprocess.check_output([cmd, "--version"], text=True).strip()
    except Exception:
        return "error"


def run_preflight() -> dict[str, list[dict[str, str]]]:
    """Execute preflight checks returning their results.

    Each entry from :mod:`scripts.preflight`'s ``CHECKS`` list is executed and
    the outcome is collected.  Successes and failures are returned in a
    dictionary matching the structure of ``preflight.json`` produced by the
    standalone preflight script.
    """

    from scripts import preflight

    successes: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    for name, func in preflight.CHECKS:
        try:
            ok, msg = func()
        except Exception as exc:  # pragma: no cover - defensive
            ok, msg = False, str(exc)
        entry = {"name": name, "message": msg}
        if ok:
            successes.append(entry)
        else:
            failures.append(entry)
    return {"successes": successes, "failures": failures}


def collect() -> dict[str, Any]:
    info: dict[str, Any] = {}
    info["python"] = platform.python_version()

    try:
        import torch  # type: ignore

        info["torch"] = getattr(torch, "__version__", "unknown")
    except Exception:
        info["torch"] = "not installed"

    try:
        from solhunter_zero import device

        info["gpu_backend"] = device.get_gpu_backend() or "none"
        gpu_device = os.environ.get("SOLHUNTER_GPU_DEVICE")
        if not gpu_device:
            try:
                gpu_device = str(device.get_default_device())
            except Exception:
                gpu_device = "none"
        info["gpu_device"] = gpu_device
    except Exception:
        info["gpu_backend"] = "unknown"
        info["gpu_device"] = "unknown"

    info["rustc"] = _rust_version("rustc")
    info["cargo"] = _rust_version("cargo")

    config_present = any(
        Path(name).is_file() for name in ("config.toml", "config.yaml", "config.yml")
    )
    info["config"] = "present" if config_present else "missing"

    try:
        from solhunter_zero import wallet

        keypairs = wallet.list_keypairs()
        active = wallet.get_active_keypair_name()
        if active:
            info["keypair"] = active
        elif keypairs:
            info["keypair"] = keypairs[0]
        else:
            info["keypair"] = "missing"
    except Exception:
        info["keypair"] = "error"

    try:
        info["preflight"] = run_preflight()
    except Exception:  # pragma: no cover - best effort only
        info["preflight"] = {"successes": [], "failures": []}

    return info


def main() -> int:
    info = collect()
    for key, val in info.items():
        print(f"{key}: {val}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
