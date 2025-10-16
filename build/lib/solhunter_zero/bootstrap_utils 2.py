from __future__ import annotations

"""Utilities for bootstrapping the Solhunter Zero environment.

This module provides helper functions for dependency management, virtual
environment creation, and other setup tasks required before running the
application.
"""

import json
import importlib.util
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

from dataclasses import dataclass
from typing import Sequence

from scripts import deps
from . import device
from .device import METAL_EXTRA_INDEX
from .logging_utils import log_startup
from .paths import ROOT
from solhunter_zero.preflight_utils import check_internet

DEPS_MARKER = ROOT / ".cache" / "deps-installed"
VENV_DIR = ROOT / ".venv"

PY_MAJOR, PY_MINOR = sys.version_info[:2]
PY_VERSION = f"{PY_MAJOR}.{PY_MINOR}"
PYTHON_NAME = f"python{PY_VERSION}"
BREW_FORMULA = f"python@{PY_VERSION}"


@dataclass
class DepsConfig:
    """Configuration for :func:`ensure_deps`."""

    install_optional: bool = False
    extras: Sequence[str] | None = ("uvloop",)
    ensure_wallet_cli: bool = True


# ---------------------------------------------------------------------------
# Virtual environment helpers
# ---------------------------------------------------------------------------


def prepend_repo_root() -> None:
    """Prepend the repository root to :data:`sys.path` if missing."""
    p = str(ROOT)
    if p not in sys.path:
        sys.path.insert(0, p)

def _venv_python() -> Path:
    """Return the path to the venv's python executable."""
    return (
        VENV_DIR
        / ("Scripts" if os.name == "nt" else "bin")
        / ("python.exe" if os.name == "nt" else "python")
    )


def _inspect_python(p: Path) -> tuple[str | None, tuple[int, int, int]]:
    """Return ``(machine, version)`` for the interpreter at *p*.

    On failure a placeholder ``(None, (0, 0, 0))`` is returned and the error is
    printed to stdout.
    """

    try:
        info = subprocess.check_output(
            [
                str(p),
                "-c",
                (
                    "import json, platform, sys;"
                    "print(json.dumps({'machine': platform.machine(), 'version': sys.version_info[:3]}))"
                ),
            ],
            text=True,
        )
        data = json.loads(info)
        return data.get("machine"), tuple(data.get("version", []))
    except Exception as exc:  # pragma: no cover - hard failure
        msg = f"Failed to inspect virtual environment interpreter: {exc}"
        print(msg)
        log_startup(msg)
        return None, (0, 0, 0)


def _create_venv(python_exe: str) -> None:
    """Create ``.venv`` using *python_exe*.

    ``SystemExit`` is raised if the command fails.
    """

    try:
        subprocess.check_call([python_exe, "-m", "venv", str(VENV_DIR)])
    except (subprocess.CalledProcessError, OSError) as exc:
        msg = f"Failed to create .venv with {python_exe}: {exc}"
        print(msg)
        log_startup(msg)
        raise SystemExit(1)


def _venv_needs_recreation() -> tuple[str | None, str]:
    """Return ``(python_exe, reason)`` if the venv must be recreated."""

    python = _venv_python()

    if not VENV_DIR.exists():
        return sys.executable, "initial setup"

    if not python.exists() or not os.access(str(python), os.X_OK):
        return sys.executable, "missing interpreter"

    machine, version = _inspect_python(python)
    if platform.system() == "Darwin" and (machine != "arm64" or version < (PY_MAJOR, PY_MINOR)):
        brew_python = shutil.which(PYTHON_NAME)
        if not brew_python:
            msg = (
                f"{PYTHON_NAME} from Homebrew not found. "
                f"Install it with 'brew install {BREW_FORMULA}'."
            )
            print(msg)
            log_startup(msg)
            raise SystemExit(1)
        return brew_python, f"Homebrew {PYTHON_NAME}"
    if version < (PY_MAJOR, PY_MINOR):
        return sys.executable, "current interpreter"
    return None, ""


def _recreate_venv(python_exe: str) -> None:
    """Remove any existing venv and create a new one using *python_exe*."""

    shutil.rmtree(VENV_DIR, ignore_errors=True)
    _create_venv(python_exe)


def ensure_venv(argv: list[str] | None) -> None:
    """Create a local virtual environment and re-invoke the script inside it.

    If *argv* is provided, it is assumed the caller already runs inside the
    desired interpreter and no action is taken.  Tests use this to bypass the
    re-exec behaviour.
    """

    if argv is not None:
        # Guard: ``argv`` is only non-``None`` when the caller explicitly wants
        # to skip virtual environment creation, e.g. during tests.
        return

    # If another virtual environment is already active, skip creating or
    # re-executing into ``.venv``.  This allows running inside a pre-existing
    # environment while still making the repository importable.
    if sys.prefix != sys.base_prefix or os.getenv("VIRTUAL_ENV"):
        prepend_repo_root()
        return

    python_exe, reason = _venv_needs_recreation()
    if python_exe:
        if reason == "initial setup":
            msg = "Creating virtual environment in .venv..."
        elif reason == "missing interpreter":
            msg = "Virtual environment missing interpreter; recreating .venv..."
        else:
            msg = f"Recreating .venv using {reason}..."
        print(msg)
        log_startup(msg)
        _recreate_venv(python_exe)

    python = _venv_python()

    if Path(sys.prefix) != VENV_DIR:
        try:
            os.execv(str(python), [str(python), *sys.argv])
        except OSError as exc:
            msg = f"Failed to execv {python}: {exc}"
            logging.exception(msg)
            log_startup(msg)
            raise






def _pip_install(*args: str, retries: int = 3) -> None:
    """Run ``pip install`` with retries and exponential backoff."""

    errors: list[str] = []
    cmd: list[str] = []
    for attempt in range(1, retries + 1):
        cmd = [sys.executable, "-m", "pip", "install", *args]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        log_startup(f"{' '.join(cmd)} (attempt {attempt}/{retries})")
        if result.stdout:
            log_startup(result.stdout.rstrip())
        if result.stderr:
            log_startup(result.stderr.rstrip())
        if result.returncode == 0:
            log_startup(
                f"pip install {' '.join(args)} succeeded on attempt {attempt}"
            )
            return
        errors.append(result.stderr.strip() or result.stdout.strip())
        if attempt < retries:
            wait = 2 ** (attempt - 1)
            msg = (
                f"pip install {' '.join(args)} failed (attempt {attempt}/{retries}). Retrying in {wait} seconds..."
            )
            print(msg)
            log_startup(msg)
            time.sleep(wait)
    msg = f"Failed to install {' '.join(args)} after {retries} attempts"
    retry_cmd = " ".join(cmd)
    end_msg = f"{msg}. To retry manually, run: {retry_cmd}"
    print(end_msg)
    log_startup(end_msg)
    log_startup(
        json.dumps({
            "event": "pip_install_failed",
            "cmd": retry_cmd,
            "errors": [e for e in errors if e],
        })
    )
    raise SystemExit(result.returncode)


def _package_missing(pkg: str) -> bool:
    """Return ``True`` if *pkg* is not installed.

    The check uses ``pip show`` which is fast and avoids unnecessary
    installations.  When a package is already present a message is logged to
    ``startup.log`` for transparency.
    """

    result = subprocess.run(
        [sys.executable, "-m", "pip", "show", pkg],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode == 0:
        log_startup(f"Skipping installation of {pkg}; already satisfied")
        return False
    return True


def ensure_deps(
    cfg: DepsConfig | None = None,
    *,
    install_optional: bool = False,
    extras: Sequence[str] | None = ("uvloop",),
    ensure_wallet_cli: bool = True,
    full: bool | None = None,
) -> None:
    """Ensure Python dependencies and optional extras are installed.

    Parameters
    ----------
    cfg:
        Optional :class:`DepsConfig` describing installation behaviour. When
        omitted, legacy keyword arguments are used to build a configuration.
    install_optional, extras, ensure_wallet_cli:
        Deprecated keyword arguments retained for backward compatibility when
        *cfg* is not provided.
    full:
        When ``True`` install optional dependencies. Deprecated alias for
        ``install_optional``.
    """

    if full is not None:
        install_optional = full

    if cfg is None:
        cfg = DepsConfig(
            install_optional=install_optional,
            extras=extras,
            ensure_wallet_cli=ensure_wallet_cli,
        )
    elif full is not None:
        cfg.install_optional = full

    urls: list[str] = []
    url = os.getenv("BROKER_URL")
    if url:
        urls.append(url)
    more = os.getenv("BROKER_URLS")
    if more:
        urls.extend(u.strip() for u in more.split(",") if u.strip())

    needs_redis = any(u.startswith(("redis://", "rediss://")) for u in urls)
    needs_nats = any(u.startswith("nats://") for u in urls)

    if needs_redis and shutil.which("redis-server") is None:
        msg = (
            "BROKER_URL is set to use Redis but 'redis-server' was not found. "
            "Install/start Redis or set BROKER_URL=memory://."
        )
        print(msg)
        log_startup(msg)
        raise SystemExit(1)

    if platform.system() == "Darwin":
        from . import macos_setup

        if not macos_setup.mac_setup_completed():
            report = macos_setup.prepare_macos_env(non_interactive=True)
            if not report.get("success"):
                for step, info in report.get("steps", {}).items():
                    if info.get("status") == "error":
                        fix = macos_setup.MANUAL_FIXES.get(step)
                        if fix:
                            print(f"Manual fix for {step}: {fix}")
                print(
                    "macOS environment preparation failed. Please address the issues above and re-run.",
                )
                raise SystemExit(1)

    force_reinstall = os.getenv("SOLHUNTER_FORCE_DEPS") == "1"
    if DEPS_MARKER.exists() and not force_reinstall:
        log_startup("Dependency marker present; skipping installation")
        from . import bootstrap as bootstrap_mod

        bootstrap_mod.ensure_target("route_ffi")
        bootstrap_mod.ensure_target("depth_service")
        return

    req, opt = deps.check_deps()
    broker_pkgs: list[str] = []
    if needs_redis and _package_missing("redis"):
        broker_pkgs.append("redis")
        if "redis" in opt:
            opt.remove("redis")
    if needs_nats and _package_missing("nats-py"):
        broker_pkgs.append("nats-py")
        if "nats" in opt:
            opt.remove("nats")
    if req:
        print("Missing required modules: " + ", ".join(req))
    if opt:
        formatted = [
            "psutil (resource monitoring disabled)" if m == "psutil" else m
            for m in opt
        ]
        print(
            "Optional modules missing: "
            + ", ".join(formatted)
            + " (features disabled).",
        )

    # Filter out packages that are already satisfied according to pip.
    req = [m for m in req if _package_missing(m.replace("_", "-"))]
    if cfg.install_optional:
        opt = [m for m in opt if _package_missing(m.replace("_", "-"))]
    else:
        opt = []

    need_cli = cfg.ensure_wallet_cli and shutil.which("solhunter-wallet") is None
    need_install = bool(req) or need_cli or broker_pkgs or (cfg.install_optional and opt)
    if not need_install:
        from . import bootstrap as bootstrap_mod

        bootstrap_mod.ensure_target("route_ffi")
        bootstrap_mod.ensure_target("depth_service")
        DEPS_MARKER.parent.mkdir(parents=True, exist_ok=True)
        DEPS_MARKER.write_text(
            json.dumps(
                {
                    "extras": list(cfg.extras) if cfg.extras else [],
                    "install_optional": cfg.install_optional,
                    "timestamp": time.time(),
                }
            )
        )
        return

    pip_check = subprocess.run(
        [sys.executable, "-m", "pip", "--version"],
        capture_output=True,
        text=True,
    )
    if pip_check.returncode != 0:
        if "No module named pip" in pip_check.stderr:
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "ensurepip", "--default-pip"]
                )
            except subprocess.CalledProcessError as exc:  # pragma: no cover - hard failure
                print(f"Failed to bootstrap pip: {exc}")
                raise SystemExit(exc.returncode)
            else:
                subprocess.check_call([sys.executable, "-m", "pip", "--version"])
        else:  # pragma: no cover - unexpected failure
            print(f"Failed to invoke pip: {pip_check.stderr.strip()}")
            raise SystemExit(pip_check.returncode)

    extra_index: list[str] = []
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        extra_index = list(METAL_EXTRA_INDEX)

    need_install = bool(req) or need_cli or broker_pkgs or (cfg.install_optional and (opt or extra_index))
    if need_install:
        import contextlib
        import io

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()
        ):
            ok, _msg = check_internet()
        if not ok:
            raise SystemExit(
                "Unable to establish an internet connection; aborting."
            )

    if req or need_cli:
        print("Installing required dependencies...")
        extras_arg = ""
        if cfg.extras:
            extras_arg = f".[{','.join(cfg.extras)}]"
        else:
            extras_arg = "."
        _pip_install(extras_arg, *extra_index)
        if need_cli:
            _pip_install("solhunter-wallet")
        failed_req = [m for m in req if importlib.util.find_spec(m) is None]
        if failed_req:
            print(
                "Missing required dependencies after installation: "
                + ", ".join(failed_req)
            )
            raise SystemExit(1)
        if need_cli and shutil.which("solhunter-wallet") is None:
            print(
                "'solhunter-wallet' still not available after installation. "
                "Please install it manually with 'pip install solhunter-wallet' "
                "and ensure it is in your PATH."
            )
            raise SystemExit(1)
    if broker_pkgs:
        print("Installing broker dependencies...")
        for pkg in broker_pkgs:
            _pip_install(pkg, *extra_index)


    if cfg.install_optional and extra_index:
        try:
            device.initialize_gpu()
        except Exception as exc:
            print(str(exc))
            raise SystemExit(str(exc))
        if "torch" in opt:
            opt.remove("torch")

    if cfg.install_optional and opt:
        print("Installing optional dependencies...")
        mapping = {
            "faiss": "faiss-cpu",
            "sentence_transformers": "sentence-transformers",
            "torch": "torch",
            "orjson": "orjson",
            "lz4": "lz4",
            "zstandard": "zstandard",
            "msgpack": "msgpack",
            "redis": "redis",
            "nats": "nats-py",
        }
        mods = set(opt)
        extras_pkgs: list[str] = []
        if "orjson" in mods and _package_missing("fastjson"):
            extras_pkgs.append("fastjson")
        if {"lz4", "zstandard"} & mods and _package_missing("fastcompress"):
            extras_pkgs.append("fastcompress")
        if "msgpack" in mods and _package_missing("msgpack"):
            extras_pkgs.append("msgpack")
        if extras_pkgs:
            _pip_install(f".[{','.join(extras_pkgs)}]", *extra_index)
        remaining = mods - {"orjson", "lz4", "zstandard", "msgpack"}
        for name in remaining:
            pkg = mapping.get(name, name.replace("_", "-"))
            if _package_missing(pkg):
                _pip_install(pkg, *extra_index)

        missing_opt = [m for m in opt if importlib.util.find_spec(m) is None]
        if missing_opt:
            formatted = [
                "psutil (resource monitoring disabled)" if m == "psutil" else m
                for m in missing_opt
            ]
            print(
                "Optional modules missing: "
                + ", ".join(formatted)
                + " (features disabled)."
            )

    from . import bootstrap as bootstrap_mod
    bootstrap_mod.ensure_target("route_ffi")
    bootstrap_mod.ensure_target("depth_service")

    DEPS_MARKER.parent.mkdir(parents=True, exist_ok=True)
    DEPS_MARKER.write_text(
        json.dumps(
            {
                "extras": list(cfg.extras) if cfg.extras else [],
                "install_optional": cfg.install_optional,
                "timestamp": time.time(),
            }
        )
    )


def ensure_endpoints(cfg: dict) -> None:
    """Ensure HTTP and WebSocket endpoints in ``cfg`` are reachable.

    The configuration may specify several service URLs such as
    ``DEX_BASE_URL`` or custom metrics endpoints. This function attempts a
    ``HEAD`` request to each HTTP(S) URL and a WebSocket handshake for
    ``ws://`` or ``wss://`` URLs, aborting startup if any service is
    unreachable. BirdEye is only checked when an API key is configured.
    """

    import asyncio
    import urllib.error
    from urllib.parse import urlparse
    from solhunter_zero.http import check_endpoint

    urls: dict[str, tuple[str, dict[str, str] | None, str, str]] = {}
    if cfg.get("birdeye_api_key"):
        urls["BirdEye"] = (
            "https://public-api.birdeye.so/defi/tokenlist",
            None,
            "https",
            "public-api.birdeye.so",
        )
    for key, val in cfg.items():
        if not isinstance(val, str):
            continue
        if not val.startswith(("http://", "https://", "ws://", "wss://")):
            continue
        headers: dict[str, str] | None = None
        if key == "jito_ws_url" and cfg.get("jito_ws_auth"):
            headers = {"Authorization": cfg["jito_ws_auth"]}
        parsed = urlparse(val)
        scheme = (parsed.scheme or "").lower()
        host = parsed.hostname or ""
        if scheme in {"ws", "wss"} and host in {"127.0.0.1", "localhost"}:
            # Local websocket endpoints are started later in the startup flow.
            continue
        urls[key] = (val, headers, scheme, host)

    async def _check(
        name: str,
        url: str,
        headers: dict[str, str] | None,
        scheme: str,
        host: str,
    ) -> tuple[str, str, Exception, bool] | None:
        # Each URL is checked with its own exponential backoff.
        for attempt in range(3):
            try:
                if url.startswith(("ws://", "wss://")):
                    import websockets

                    async with websockets.connect(
                        url, extra_headers=headers, open_timeout=5
                    ):
                        return None
                # ``check_endpoint`` is synchronous; run it in a thread to avoid blocking.
                await asyncio.to_thread(check_endpoint, url, retries=1)
                return None
            except Exception as exc:  # pragma: no cover - network failure
                if attempt == 2:
                    soft = True
                    if scheme in {"http", "https"} and isinstance(exc, urllib.error.HTTPError):
                        soft = exc.code < 500
                    return name, url, exc, soft
                wait = 2**attempt
                print(
                    f"Attempt {attempt + 1} failed for {name} at {url}: {exc}. "
                    f"Retrying in {wait} seconds..."
                )
                await asyncio.sleep(wait)

    async def _run() -> list[tuple[str, str, Exception, bool] | None]:
        tasks = [
            _check(name, url, headers, scheme, host)
            for name, (url, headers, scheme, host) in urls.items()
        ]
        return await asyncio.gather(*tasks)

    results = asyncio.run(_run())
    failures: list[tuple[str, str, Exception]] = []
    warnings: list[tuple[str, str, Exception]] = []
    for name_exc in results:
        if name_exc is None:
            continue
        name, url, exc, soft = name_exc
        if soft:
            warnings.append((name, url, exc))
        else:
            failures.append((name, url, exc))

    if failures:
        details = "; ".join(
            f"{name} at {url} ({exc})" for name, url, exc in failures
        )
        print(
            "Failed to reach the following endpoints: "
            f"{details}. Check your network connection or configuration."
        )
        raise SystemExit(1)

    if warnings:
        warn_details = "; ".join(
            f"{name} at {url} ({exc})" for name, url, exc in warnings
        )
        print(
            "Warning: the following endpoints responded but returned non-success "
            f"status codes: {warn_details}"
        )


def _run_rustup_setup(cmd, *, shell: bool = False, retries: int = 2) -> None:
    """Run ``cmd`` retrying on failure with helpful errors."""

    for attempt in range(1, retries + 1):
        try:
            subprocess.check_call(cmd, shell=shell)
            return
        except subprocess.CalledProcessError as exc:
            if attempt == retries:
                print(
                    "Failed to install Rust toolchain via rustup. "
                    "Please visit https://rustup.rs/ and follow the instructions.",
                )
                raise SystemExit(exc.returncode)
            time.sleep(1)
            print("Rustup setup failed, retrying...")


def ensure_cargo() -> None:
    installed = False
    cache_marker = ROOT / ".cache" / "cargo-installed"
    if platform.system() == "Darwin":
        from solhunter_zero.macos_setup import apply_brew_env, ensure_tools

        ensure_tools()
        apply_brew_env()

    cargo_bin = Path.home() / ".cargo" / "bin"
    os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    if shutil.which("cargo") is None:
        if cache_marker.exists():
            print(
                "Rust toolchain previously installed but 'cargo' was not found. "
                "Ensure ~/.cargo/bin is in your PATH or remove the cache marker and rerun the script.",
            )
            raise SystemExit(1)

        cache_marker.parent.mkdir(parents=True, exist_ok=True)
        if shutil.which("brew") is not None:
            print("Installing rustup with Homebrew...")
            try:
                subprocess.check_call(["brew", "install", "rustup"])
            except subprocess.CalledProcessError as exc:
                print(f"Homebrew failed to install rustup: {exc}")
                raise SystemExit(exc.returncode)
            _run_rustup_setup(["rustup-init", "-y"])
        else:
            if shutil.which("curl") is None:
                print(
                    "curl is required to install the Rust toolchain. "
                    "Install it (e.g., with Homebrew: 'brew install curl') and re-run this script.",
                )
                raise SystemExit(1)
            print("Installing Rust toolchain via rustup...")
            _run_rustup_setup(
                "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
                shell=True,
            )
        installed = True
        cache_marker.write_text("ok")

    try:
        subprocess.check_call(["cargo", "--version"], stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError as exc:
        print("Failed to run 'cargo --version'. Is Rust installed correctly?")
        raise SystemExit(exc.returncode)
    if installed and platform.system() == "Darwin" and platform.machine() == "arm64":
        subprocess.check_call(["rustup", "target", "add", "aarch64-apple-darwin"])
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        try:
            targets = subprocess.check_output(["rustup", "target", "list"], text=True)
        except subprocess.CalledProcessError as exc:
            print("Failed to list rust targets. Is rustup installed correctly?")
            raise SystemExit(exc.returncode)
        if "aarch64-apple-darwin" not in targets:
            subprocess.check_call(["rustup", "target", "add", "aarch64-apple-darwin"])

    missing = [tool for tool in ("pkg-config", "cmake") if shutil.which(tool) is None]
    if missing:
        if platform.system() == "Darwin" and shutil.which("brew") is not None:
            print(
                f"Missing {', '.join(missing)}. Attempting to install with Homebrew...",
            )
            try:
                subprocess.check_call(["brew", "install", "pkg-config", "cmake"])
            except subprocess.CalledProcessError as exc:
                print(f"Homebrew installation failed: {exc}")
            else:
                missing = [
                    tool
                    for tool in ("pkg-config", "cmake")
                    if shutil.which(tool) is None
                ]
        if missing:
            names = " and ".join(missing)
            brew = " ".join(missing)
            print(
                f"{names} {'are' if len(missing) > 1 else 'is'} required to build native extensions. "
                f"Install {'them' if len(missing) > 1 else 'it'} (e.g., with Homebrew: 'brew install {brew}') and re-run this script.",
            )
            raise SystemExit(1)
