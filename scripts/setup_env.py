#!/usr/bin/env python3
"""Environment setup utility for Solsniper-zero.

This script bootstraps a development environment by ensuring a
virtual environment is active, installing dependencies and tooling,
building the Rust FFI extension, and launching services when complete.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import venv

ROOT = Path(__file__).resolve().parent.parent


def run(cmd: list[str], **kwargs) -> None:
    """Run a subprocess and stream output."""
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True, **kwargs)


def ensure_venv() -> None:
    """Ensure we're running inside a virtualenv, creating one if needed."""
    active_venv = sys.prefix != sys.base_prefix or bool(os.getenv("VIRTUAL_ENV"))
    if active_venv:
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        return
    venv_dir = ROOT / ".venv"
    if not venv_dir.exists():
        print(f"Creating venv at {venv_dir}")
        venv.create(venv_dir, with_pip=True)
    python_bin = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"
    print(f"Re-executing under {python_bin}")
    os.execv(str(python_bin), [str(python_bin), __file__] + sys.argv[1:])


def pip_install(args: list[str]) -> None:
    run([sys.executable, "-m", "pip", "install", *args])


def install_project() -> None:
    extras = "fastjson,fastcompress,msgpack"
    packages = [
        f".[{extras}]",
        "protobuf",
        "grpcio-tools",
        "scikit-learn",
        "PyYAML",
        "redis",
        "psutil",
    ]
    pip_install(["-e", *packages])


def install_brew_packages() -> None:
    if sys.platform != "darwin":
        return
    py_ver = f"python@{sys.version_info.major}.{sys.version_info.minor}"
    pkgs = [py_ver, "rustup", "pkg-config", "cmake", "protobuf"]
    run(["brew", "install", *pkgs])


def configure_pytorch(cfg: dict) -> None:
    torch_cfg = cfg.get("torch", {}) if isinstance(cfg, dict) else {}
    torch_ver = torch_cfg.get("torch_metal_version")
    tv_ver = torch_cfg.get("torchvision_metal_version")
    specs = []
    if torch_ver:
        specs.append(f"torch=={torch_ver}")
    else:
        specs.append("torch")
    if tv_ver:
        specs.append(f"torchvision=={tv_ver}")
    else:
        specs.append("torchvision")
    args = []
    if sys.platform == "darwin":
        args += ["--index-url", "https://download.pytorch.org/whl/metal"]
    args += specs
    pip_install(args)


def build_route_ffi() -> None:
    crate_dir = ROOT / "route_ffi"
    if not crate_dir.exists():
        return
    run(
        [
            "cargo",
            "build",
            "--release",
            "--features=parallel",
            "--manifest-path",
            str(crate_dir / "Cargo.toml"),
        ]
    )
    target = crate_dir / "target" / "release"
    if sys.platform == "darwin":
        libname = "libroute_ffi.dylib"
    elif os.name == "nt":
        libname = "route_ffi.dll"
    else:
        libname = "libroute_ffi.so"
    src = target / libname
    dest = ROOT / "solhunter_zero" / libname
    if src.exists():
        shutil.copy2(src, dest)
        print(f"Copied {src} -> {dest}")
    else:
        print("Route FFI build artifact not found; set ROUTE_FFI_LIB manually.")


def preflight(cfg: dict) -> None:
    from solhunter_zero.preflight_utils import check_internet

    ok, msg = check_internet()
    if not ok:
        raise SystemExit(f"Internet check failed: {msg}")

    broker_url = os.environ.get("BROKER_URL") or cfg.get("broker_url") or cfg.get("broker_urls")
    if broker_url and "redis" in broker_url:
        if not shutil.which("redis-server"):
            if sys.platform == "darwin":
                print("redis-server not found. Installing via Homebrew.")
                try:
                    run(["brew", "install", "redis"])
                    run(["brew", "services", "start", "redis"])
                except Exception as exc:
                    raise SystemExit("Failed to install redis via Homebrew.") from exc
                if not shutil.which("redis-server"):
                    raise SystemExit(
                        "Redis broker configured but redis-server still not found."
                    )
            else:
                print("redis-server not found; using in-memory broker.")
                os.environ["BROKER_URL"] = "memory://"
                cfg["broker_url"] = "memory://"


def print_summary() -> None:
    pkgs = [
        "protobuf",
        "grpcio-tools",
        "scikit-learn",
        "PyYAML",
        "redis",
        "psutil",
        "torch",
        "torchvision",
    ]
    print("\nInstalled package versions:")
    for pkg in pkgs:
        try:
            ver = version(pkg)
        except PackageNotFoundError:
            ver = "not installed"
        print(f"  {pkg}: {ver}")


def generate_config() -> dict:
    from solhunter_zero.config_bootstrap import ensure_config, _ensure_tomli_w

    cfg_path, cfg = ensure_config()
    defaults = {
        "BROKER_WS_URLS": "ws://127.0.0.1:8779",
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY",
        "dex_base_url": "https://swap.helius.dev",
    }
    updated = False
    for key, val in defaults.items():
        if not cfg.get(key):
            cfg[key] = val
            updated = True
    if updated:
        tomli_w = _ensure_tomli_w()
        with cfg_path.open("wb") as fh:
            fh.write(tomli_w.dumps(cfg).encode("utf-8"))
    return cfg


def main() -> None:
    print(f"Python executable: {sys.executable}")
    print(f"Platform: {platform.platform()}")

    ensure_venv()
    cfg = generate_config()
    install_brew_packages()
    preflight(cfg)
    install_project()
    configure_pytorch(cfg)
    build_route_ffi()
    print_summary()

    start_all = ROOT / "scripts" / "start_all.py"
    print(f"Launching services via {start_all}...")
    result = subprocess.run([sys.executable, str(start_all)])
    if result.returncode == 0:
        print("Service launch completed successfully.")
    else:
        print(
            f"Service launch exited with code {result.returncode}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
