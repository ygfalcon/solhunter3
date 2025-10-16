#!/usr/bin/env python3
"""Compatibility wrapper for macOS environment setup.

This script now also resolves the appropriate ``torch`` and ``torchvision``
Metal wheel versions for the running Python interpreter.  The discovered
versions are written back to ``config.toml`` so subsequent runs can reuse
them without hitting the network.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List

import requests
import tomli_w

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for older versions
    import tomli as tomllib  # type: ignore

from packaging import version

from solhunter_zero.macos_setup import *  # noqa: F401,F403

METAL_INDEX = "https://download.pytorch.org/whl/metal"
ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config.toml"


def _available_versions(pkg: str, py_tag: str) -> List[version.Version]:
    """Return sorted Metal wheel versions for *pkg* matching *py_tag*."""

    url = f"{METAL_INDEX}/{pkg}/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    pattern = rf"{pkg}-(\\d+\.\\d+(?:\.\\d+)?)" \
              rf"[^\s]*-{py_tag}"
    return sorted(
        version.parse(m.group(1))
        for m in re.finditer(pattern, resp.text)
    )


def resolve_metal_versions() -> tuple[str, str]:
    """Resolve torch and torchvision Metal wheel versions for this Python."""

    py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    torch_versions = _available_versions("torch", py_tag)
    vision_versions = _available_versions("torchvision", py_tag)
    if not torch_versions or not vision_versions:
        raise RuntimeError(f"No Metal wheels found for Python {py_tag}")
    return str(torch_versions[-1]), str(vision_versions[-1])


def write_versions_to_config(torch_ver: str, vision_ver: str) -> None:
    """Write resolved versions back to ``config.toml``."""

    cfg = {}
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "rb") as f:
            cfg = tomllib.load(f)
    torch_cfg = cfg.setdefault("torch", {})
    torch_cfg["torch_metal_version"] = torch_ver
    torch_cfg["torchvision_metal_version"] = vision_ver
    with open(CONFIG_PATH, "wb") as f:
        tomli_w.dump(cfg, f)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    try:
        torch_v, vision_v = resolve_metal_versions()
        write_versions_to_config(torch_v, vision_v)
    except Exception:  # pragma: no cover - best effort
        pass
    from solhunter_zero.macos_setup import main

    main()

