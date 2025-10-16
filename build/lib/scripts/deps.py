#!/usr/bin/env python3
"""Dependency checking utilities for SolHunter Zero."""

from __future__ import annotations

import argparse
import os
import pkgutil
import re
import subprocess
import sys

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - should not happen
    import tomli as tomllib  # type: ignore

from solhunter_zero.paths import ROOT
from solhunter_zero import device

# Map distribution names to the module names they provide when imported. This is
# necessary for packages whose import name differs from the name used on PyPI.
IMPORT_NAME_MAP = {
    "scikit-learn": "sklearn",
    "pyyaml": "yaml",
    "pytorch-lightning": "pytorch_lightning",
    "faiss-cpu": "faiss",
    "opencv-python": "cv2",
    "beautifulsoup4": "bs4",
    "grpcio-tools": "grpc_tools",
    "protobuf": "google.protobuf",
    "nats-py": "nats",
}

OPTIONAL_DEPS = [
    "faiss",
    "sentence_transformers",
    "torch",
    "orjson",
    "lz4",
    "zstandard",
    "msgpack",
    "psutil",
    "redis",
    "nats-py",
]


def check_deps() -> tuple[list[str], list[str]]:
    """Return lists of missing required and optional modules."""
    with open(ROOT / "pyproject.toml", "rb") as fh:
        data = tomllib.load(fh)
    deps = data.get("project", {}).get("dependencies", [])
    build_deps = data.get("build-system", {}).get("requires", [])
    for dep in build_deps:
        if dep.startswith("grpcio-tools"):
            deps.append(dep)
    missing_required: list[str] = []
    for dep in deps:
        dist_name = re.split("[<=>]", dep)[0]
        mod = IMPORT_NAME_MAP.get(dist_name, dist_name).replace("-", "_")
        if pkgutil.find_loader(mod) is None:
            missing_required.append(mod)

    missing_optional = []
    for dep in OPTIONAL_DEPS:
        mod = IMPORT_NAME_MAP.get(dep, dep).replace("-", "_")
        if pkgutil.find_loader(mod) is None:
            missing_optional.append(mod)

    return missing_required, missing_optional


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Install project dependencies")
    parser.add_argument(
        "--install-optional",
        action="store_true",
        help="Install optional dependencies",
    )
    parser.add_argument(
        "--extras",
        nargs="*",
        help="Extras to install from the local package",
    )
    args = parser.parse_args(argv)

    from solhunter_zero.bootstrap_utils import DepsConfig, ensure_deps

    cfg = DepsConfig(
        install_optional=args.install_optional,
        extras=args.extras if args.extras else ("uvloop",),
    )
    ensure_deps(cfg)

    if "PYTEST_CURRENT_TEST" not in os.environ:
        METAL_INDEX = (
            device.METAL_EXTRA_INDEX[1]
            if len(getattr(device, "METAL_EXTRA_INDEX", [])) > 1
            else "https://download.pytorch.org/whl/metal"
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                ".[fastjson,fastcompress,msgpack]",
                "--extra-index-url",
                METAL_INDEX,
            ]
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
