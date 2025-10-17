#!/usr/bin/env python3
"""Dependency checking utilities for SolHunter Zero."""

from __future__ import annotations

import argparse
import os
import pkgutil
import platform
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
        "--check-only",
        action="store_true",
        help="Only report missing dependencies; do not install",
    )
    parser.add_argument(
        "--extras",
        nargs="*",
        help="Extras to install from the local package",
    )
    args = parser.parse_args(argv)

    from solhunter_zero.bootstrap_utils import DepsConfig, ensure_deps

    pyproject = ROOT / "pyproject.toml"
    if not pyproject.exists():
        print(f"[deps] pyproject.toml not found at {pyproject}", file=sys.stderr)
        return 2

    try:
        missing_required, missing_optional = check_deps()
    except tomllib.TOMLDecodeError as exc:  # pragma: no cover - configuration error
        print(f"[deps] failed to parse {pyproject}: {exc}", file=sys.stderr)
        return 2

    if args.check_only:
        if missing_required:
            print("[deps] missing required:", ", ".join(sorted(missing_required)))
        if args.install_optional and missing_optional:
            print("[deps] missing optional:", ", ".join(sorted(missing_optional)))
        return 1 if missing_required else 0

    cfg = DepsConfig(
        install_optional=args.install_optional,
        extras=
        tuple(args.extras)
        if args.extras
        else (() if platform.system().lower().startswith("win") else ("uvloop",)),
    )
    ensure_deps(cfg)

    if "PYTEST_CURRENT_TEST" not in os.environ:
        pip_args = [
            sys.executable,
            "-m",
            "pip",
            "install",
            ".[fastjson,fastcompress,msgpack]",
            "--disable-pip-version-check",
            "--no-input",
            "--root-user-action=ignore",
        ]
        if platform.system() == "Darwin" and platform.machine() in {"arm64", "aarch64"}:
            metal_list = getattr(device, "METAL_EXTRA_INDEX", [])
            METAL_INDEX = (
                metal_list[1]
                if isinstance(metal_list, (list, tuple)) and len(metal_list) > 1
                else "https://download.pytorch.org/whl/metal"
            )
            pip_args += ["--extra-index-url", METAL_INDEX]
        result = subprocess.run(pip_args, cwd=str(ROOT))
        if result.returncode != 0:
            print("[deps] pip extras install failed", file=sys.stderr)
            return result.returncode

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
