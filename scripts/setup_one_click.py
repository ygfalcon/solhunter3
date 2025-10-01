#!/usr/bin/env python3
"""Perform a one-click setup and launch for SolHunter Zero."""

from __future__ import annotations

import importlib
import importlib.resources as resources
import os
import sys
import subprocess
import shutil
import site
from packaging.version import InvalidVersion, Version
import tomllib
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

import solhunter_zero  # noqa: F401

from solhunter_zero.macos_setup import ensure_tools, _resolve_metal_versions, _write_versions_to_config
import solhunter_zero.env_config as env_config
from solhunter_zero.paths import ROOT
from scripts import quick_setup
from solhunter_zero.logging_utils import log_startup
from solhunter_zero import wallet
from solhunter_zero.event_bus import DEFAULT_WS_URL


REQUIRED_CFG_KEYS = {
    "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    "dex_base_url": "https://quote-api.jup.ag",
    "agents": ["simulation"],
    "agent_weights": {"simulation": 1.0},
}


def _validate_config(path: os.PathLike[str]) -> None:
    """Ensure the configuration file contains required settings."""
    with open(path, "rb") as fh:
        cfg = tomllib.load(fh)

    missing = [k for k in REQUIRED_CFG_KEYS if k not in cfg or not cfg[k]]
    if missing:
        print(
            f"Missing required keys in {path}: {', '.join(missing)}",
            file=sys.stderr,
        )
        example = (
            "solana_rpc_url = \"https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d\"\n"
            "dex_base_url = \"https://quote-api.jup.ag\"\n"
            "agents = [\"simulation\"]\n\n"
            "[agent_weights]\n"
            "simulation = 1.0\n"
        )
        print("Example configuration:\n\n" + example, file=sys.stderr)
        sys.exit(1)


def _purge_corrupt_install() -> None:
    """Remove broken solhunter-zero installs before reinstalling."""
    res = subprocess.run(
        [sys.executable, "-m", "pip", "show", "solhunter-zero"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return
    version: str | None = None
    location: str | None = None
    for line in res.stdout.splitlines():
        if line.startswith("Version:"):
            version = line.split(":", 1)[1].strip()
        elif line.startswith("Location:"):
            location = line.split(":", 1)[1].strip()
    if not version:
        return
    try:
        Version(version)
        return
    except InvalidVersion:
        pass

    dirs: set[Path] = set()
    if location:
        dirs.add(Path(location))
    dirs.update(Path(p) for p in site.getsitepackages())
    dirs.add(Path(site.getusersitepackages()))
    for base in dirs:
        for target in base.glob("solhunter_zero*"):
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)


def main(argv: list[str] | None = None) -> None:
    """Execute the automated setup steps then run the autopilot."""
    ensure_tools(non_interactive=True)
    repo_root = ROOT if "site-packages" not in str(ROOT) else Path.cwd()
    env_config.configure_environment(repo_root)
    quick_setup.main(["--auto", "--non-interactive"])
    cfg_path = getattr(quick_setup, "CONFIG_PATH", None)
    if cfg_path:
        _validate_config(cfg_path)
        os.environ["SOLHUNTER_CONFIG"] = str(cfg_path)
    env_file = repo_root / ".env"

    torch_ver = os.getenv("TORCH_METAL_VERSION")
    vision_ver = os.getenv("TORCHVISION_METAL_VERSION")
    cfg_has_versions = False
    if cfg_path and Path(cfg_path).exists():
        with open(cfg_path, "rb") as fh:
            cfg = tomllib.load(fh)
        torch_cfg = cfg.get("torch", {})
        cfg_torch = torch_cfg.get("torch_metal_version")
        cfg_vision = torch_cfg.get("torchvision_metal_version")
        if cfg_torch and cfg_vision:
            cfg_has_versions = True
        torch_ver = torch_ver or cfg_torch
        vision_ver = vision_ver or cfg_vision
    if not (torch_ver and vision_ver):
        torch_ver, vision_ver = _resolve_metal_versions()
    os.environ.setdefault("TORCH_METAL_VERSION", torch_ver)
    os.environ.setdefault("TORCHVISION_METAL_VERSION", vision_ver)
    if cfg_path and Path(cfg_path).exists() and not cfg_has_versions:
        _write_versions_to_config(torch_ver, vision_ver)
    env_lines = env_file.read_text().splitlines(True) if env_file.exists() else []
    changed = False
    for prefix, value in (
        ("TORCH_METAL_VERSION", torch_ver),
        ("TORCHVISION_METAL_VERSION", vision_ver),
    ):
        if not any(line.startswith(f"{prefix}=") for line in env_lines):
            env_lines.append(f"{prefix}={value}\n")
            changed = True
    if changed:
        env_file.write_text("".join(env_lines))
    importlib.invalidate_caches()
    device = importlib.import_module("solhunter_zero.device")

    event_url = os.environ.setdefault("EVENT_BUS_URL", DEFAULT_WS_URL)
    broker_urls = os.environ.setdefault("BROKER_WS_URLS", event_url)

    lines = env_file.read_text().splitlines(True)
    seen_event = False
    seen_broker = False
    seen_cfg = False
    for i, line in enumerate(lines):
        if line.startswith("EVENT_BUS_URL="):
            lines[i] = f"EVENT_BUS_URL={event_url}\n"
            seen_event = True
        elif line.startswith("BROKER_WS_URLS="):
            lines[i] = f"BROKER_WS_URLS={broker_urls}\n"
            seen_broker = True
        elif line.startswith("SOLHUNTER_CONFIG=") and cfg_path:
            lines[i] = f"SOLHUNTER_CONFIG={cfg_path}\n"
            seen_cfg = True
    if not seen_event:
        lines.append(f"EVENT_BUS_URL={event_url}\n")
    if not seen_broker:
        lines.append(f"BROKER_WS_URLS={broker_urls}\n")
    if cfg_path and not seen_cfg:
        lines.append(f"SOLHUNTER_CONFIG={cfg_path}\n")
    with env_file.open("w", encoding="utf-8") as fh:
        fh.writelines(lines)

    # Dependency installation is deferred to ``bootstrap.bootstrap`` which
    # runs as part of the autopilot startup sequence.

    event_pb2 = repo_root / "solhunter_zero" / "event_pb2.py"
    event_proto = repo_root / "proto" / "event.proto"
    if (
        not event_pb2.exists()
        or event_pb2.stat().st_mtime < event_proto.stat().st_mtime
    ):
        subprocess.check_call(
            [sys.executable, str(repo_root / "scripts" / "gen_proto.py")]
        )

    if "PYTEST_CURRENT_TEST" not in os.environ:
        _purge_corrupt_install()
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

    if shutil.which("cargo") and shutil.which("rustup"):
        try:
            subprocess.check_call(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--features=parallel",
                    "--manifest-path",
                    "route_ffi/Cargo.toml",
                ],
                cwd=repo_root,
            )

            target = repo_root / "route_ffi" / "target" / "release"
            if sys.platform == "darwin":
                libname = "libroute_ffi.dylib"
            elif os.name == "nt":
                libname = "route_ffi.dll"
            else:
                libname = "libroute_ffi.so"
            src = target / libname
            dest = repo_root / "solhunter_zero" / libname
            if src.exists():
                try:
                    shutil.copy2(src, dest)
                    os.environ["ROUTE_FFI_LIB"] = str(dest)
                    msg = f"Route FFI library available at {dest}"
                except OSError as exc:
                    os.environ["ROUTE_FFI_LIB"] = str(src)
                    msg = (
                        "Failed to copy route FFI library; using "
                        f"{src} ({exc})"
                    )
            else:
                msg = (
                    "Route FFI build artifact not found; "
                    "set ROUTE_FFI_LIB manually."
                )
            print(msg)
            log_startup(msg)
        except subprocess.CalledProcessError as exc:
            msg = "Failed to build route_ffi with parallel feature"
            print(f"{msg}: {exc}")
            log_startup(f"{msg}: {exc}")

    os.environ["AUTO_SELECT_KEYPAIR"] = "1"
    wallet.setup_default_keypair()
    device.initialize_gpu()

    start_all = resources.files("scripts") / "start_all.py"
    # Launch the full stack including the web UI. ``start_all.py`` already
    # starts the trading process, so avoid setting ``AUTO_START`` to prevent
    # the UI from launching an additional trading thread.
    os.execvp(sys.executable, [sys.executable, str(start_all)])


if __name__ == "__main__":  # pragma: no cover
    main()
