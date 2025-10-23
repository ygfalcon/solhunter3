"""Install macOS dependencies for SolHunter Zero."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path
from urllib import request

import requests
from packaging import version

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - fallback for older versions
    import tomli as tomllib  # type: ignore

import tomli_w

from .bootstrap_utils import ensure_deps
from .cache_paths import MAC_SETUP_MARKER, TOOLS_OK_MARKER
from .logging_utils import log_startup
from .paths import ROOT
from .util import parse_bool_env

METAL_INDEX = "https://download.pytorch.org/whl/metal"
METAL_EXTRA_INDEX = ["--extra-index-url", METAL_INDEX]
CONFIG_PATH = ROOT / "config.toml"


def _available_versions(pkg: str, py_tag: str) -> list[version.Version]:
    """Return sorted Metal wheel versions for *pkg* matching *py_tag*."""

    url = f"{METAL_INDEX}/{pkg}/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    pattern = rf"{pkg}-(\\d+\\.\\d+(?:\\.\\d+)?)" rf"[^\s]*-{py_tag}"
    return sorted(version.parse(m.group(1)) for m in re.finditer(pattern, resp.text))


def _resolve_metal_versions() -> tuple[str, str]:
    """Resolve torch and torchvision Metal wheel versions for this Python."""

    py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}"
    torch_versions = _available_versions("torch", py_tag)
    vision_versions = _available_versions("torchvision", py_tag)
    if not torch_versions or not vision_versions:
        raise RuntimeError(f"No Metal wheels found for Python {py_tag}")
    return str(torch_versions[-1]), str(vision_versions[-1])


def _write_versions_to_config(torch_ver: str, vision_ver: str) -> None:
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


try:  # pragma: no cover - optional import for CI
    from solhunter_zero.device import load_torch_metal_versions

    TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION = load_torch_metal_versions()
except Exception:  # pragma: no cover - network best effort
    try:
        TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION = _resolve_metal_versions()
        _write_versions_to_config(TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION)
    except Exception:
        TORCH_METAL_VERSION = ""
        TORCHVISION_METAL_VERSION = ""

REPORT_PATH = ROOT / "macos_setup_report.json"

PY_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
PY_BIN = sys.executable
PY_NAME = Path(PY_BIN).name
PY_FORMULA = f"python@{PY_VERSION}"


def _write_report(report: dict[str, object]) -> None:
    """Persist the macOS setup report to ``REPORT_PATH``."""
    try:
        REPORT_PATH.write_text(json.dumps(report, indent=2))
    except Exception:
        pass



def mac_setup_completed() -> bool:
    """Return ``True`` if the macOS setup marker exists."""
    return MAC_SETUP_MARKER.exists()


def _detect_missing_tools() -> list[str]:
    """Return a list of required macOS tools that are not available."""
    missing: list[str] = []
    try:
        if (
            subprocess.run(
                ["xcode-select", "-p"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode
            != 0
        ):
            missing.append("xcode-select")
    except FileNotFoundError:
        missing.append("xcode-select")
    for cmd in ("brew", PY_BIN, "rustup"):
        if shutil.which(cmd) is None:
            missing.append(Path(cmd).name)
    return missing




def _run(cmd: list[str], check: bool = True, **kwargs) -> subprocess.CompletedProcess[str]:
    """Run command printing it."""
    print("Running:", " ".join(cmd))
    return subprocess.run(cmd, check=check, text=True, **kwargs)


def _run_with_retry(
    cmd: list[str], *, retries: int = 3, backoff: float = 1.0
) -> None:
    """Run ``cmd`` with retries and exponential backoff.

    Retries the command ``retries`` times sleeping ``backoff`` seconds doubled
    each attempt.  If all attempts fail a ``RuntimeError`` is raised so the
    caller can surface the failure in the setup report.
    """

    for attempt in range(1, retries + 1):
        try:
            _run(cmd, check=True)
            return
        except subprocess.CalledProcessError as exc:
            if attempt == retries:
                raise RuntimeError(
                    f"command {' '.join(cmd)} failed after {retries} attempts"
                ) from exc
            sleep_for = backoff * 2 ** (attempt - 1)
            print(
                f"Command {' '.join(cmd)} failed (attempt {attempt}/{retries}). "
                f"Retrying in {sleep_for} seconds..."
            )
            time.sleep(sleep_for)


def ensure_network() -> None:
    """Abort if no network connectivity is available."""
    try:
        subprocess.run(
            ["ping", "-c", "1", "1.1.1.1"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        return
    except Exception:
        try:
            with request.urlopen("https://api.github.com", timeout=5):
                return
        except Exception:
            print(
                "Network check failed. Please connect to the internet and re-run this script.",
                file=sys.stderr,
            )
            raise SystemExit(1)


def ensure_xcode(non_interactive: bool) -> None:
    if subprocess.run(["xcode-select", "-p"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        return
    print("Installing Xcode command line tools...")
    subprocess.run(["xcode-select", "--install"], check=False)
    elapsed = 0
    timeout = 300
    interval = 10
    while subprocess.run(["xcode-select", "-p"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        if non_interactive:
            if elapsed >= timeout:
                print(f"Command line tools installation timed out after {timeout}s.", file=sys.stderr)
                raise SystemExit(1)
            time.sleep(interval)
            elapsed += interval
        else:
            ans = input(
                "Command line tools not yet installed. Press Enter to re-check or type 'c' to cancel: "
            )
            if ans.lower() == "c":
                print("Please re-run this script after the tools are installed.")
                raise SystemExit(1)

def apply_brew_env() -> None:
    """Load Homebrew environment variables into ``os.environ``."""
    try:
        out = subprocess.check_output(["brew", "shellenv"], text=True)
    except Exception:
        return
    for line in out.splitlines():
        if not line.startswith("export "):
            continue
        key, val = line[len("export ") :].split("=", 1)
        val = val.strip().strip('"')
        if key == "PATH":
            os.environ[key] = f"{val}:{os.environ.get(key, '')}"
        else:
            os.environ[key] = val


# Backwards compatibility
_apply_brew_env = apply_brew_env


def ensure_homebrew() -> None:
    if shutil.which("brew") is None:
        print("Homebrew not found. Installing...")
        _run([
            "/bin/bash",
            "-c",
            "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)",
        ])
    apply_brew_env()


def install_brew_packages() -> None:
    _run_with_retry(["brew", "update"])
    _run_with_retry(
        [
            "brew",
            "install",
            PY_FORMULA,
            "rustup-init",
            "pkg-config",
            "cmake",
            "protobuf",
        ]
    )
    apply_brew_env()


def ensure_rustup() -> None:
    if shutil.which("rustup") is None:
        _run(["rustup-init", "-y"])
        cargo_env = Path.home() / ".cargo" / "env"
        if cargo_env.exists():
            with cargo_env.open() as fh:
                for line in fh:
                    if line.startswith("export PATH="):
                        value = line.split("=", 1)[1].strip().strip('"')
                        os.environ["PATH"] = f"{value}:{os.environ.get('PATH', '')}"
                        break


def ensure_profile() -> None:
    shell = os.environ.get("SHELL", "")
    profile = Path.home() / (".zprofile" if shell.endswith("zsh") else ".bash_profile")
    content = profile.read_text().splitlines() if profile.exists() else []
    brew_env = subprocess.check_output(["brew", "shellenv"], text=True).splitlines()

    for line in brew_env:
        if line not in content:
            with profile.open("a") as fh:
                fh.write(line + "\n")
            print(f"Success: added line to {profile}: {line}")
            content.append(line)
        else:
            print(f"No change needed for {profile}: {line}")

    cargo_line = 'source "$HOME/.cargo/env"'
    if cargo_line not in content:
        with profile.open("a") as fh:
            fh.write(cargo_line + "\n")
        print(f"Success: added line to {profile}: {cargo_line}")
    else:
        print(f"No change needed for {profile}: {cargo_line}")


def upgrade_pip_and_torch() -> None:
    py = sys.executable
    if shutil.which(py) is None:
        return
    _run_with_retry([py, "-m", "pip", "install", "--upgrade", "pip"])
    if not TORCH_METAL_VERSION or not TORCHVISION_METAL_VERSION:
        return
    _run_with_retry(
        [
            py,
            "-m",
            "pip",
            "install",
            f"torch=={TORCH_METAL_VERSION}",
            f"torchvision=={TORCHVISION_METAL_VERSION}",
            *METAL_EXTRA_INDEX,
        ]
    )


def verify_tools() -> None:
    missing = []
    for tool in [sys.executable, "brew", "rustup"]:
        if shutil.which(tool) is None:
            missing.append(tool)
    if missing:
        brew_prefix = subprocess.check_output(["brew", "--prefix"], text=True).strip()
        print(
            f"Missing {' '.join(missing)} on PATH. Ensure {brew_prefix}/bin is in your PATH and re-run this script."
        )
        raise SystemExit(1)


def install_deps() -> None:
    """Install Python dependencies using :func:`ensure_deps`."""
    MAC_SETUP_MARKER.parent.mkdir(parents=True, exist_ok=True)
    MAC_SETUP_MARKER.write_text("ok")
    try:
        ensure_deps(full=True)
        return
    except Exception as exc:  # pragma: no cover - hard failure
        needs_fallback = isinstance(exc, FileNotFoundError) or "pyproject.toml" in str(exc)
        if needs_fallback:
            repo_root = ROOT if "site-packages" not in str(ROOT) else Path.cwd()
            try:
                subprocess.check_call(
                    [sys.executable, "-m", "scripts.deps", "--install-optional"],
                    cwd=repo_root,
                )
                return
            except Exception:  # pragma: no cover - fallback failure
                MAC_SETUP_MARKER.unlink(missing_ok=True)
                raise
        MAC_SETUP_MARKER.unlink(missing_ok=True)
        raise


MANUAL_FIXES = {
    "xcode": "Install Xcode command line tools with 'xcode-select --install' and rerun the script.",
    "homebrew": "Install Homebrew from https://brew.sh and ensure it is on your PATH.",
    "brew_packages": f"Run 'brew install {PY_FORMULA} rustup-init pkg-config cmake protobuf'.",
    "rustup": "Run 'rustup-init -y' and ensure '$HOME/.cargo/bin' is on your PATH.",
    "pip_torch": (
        f"Ensure {PY_NAME} is installed then run "
        f"'{PY_BIN} -m pip install --upgrade pip "
        f"torch=={TORCH_METAL_VERSION} torchvision=={TORCHVISION_METAL_VERSION} {' '.join(METAL_EXTRA_INDEX)}'."
    ),
    "verify_tools": "Ensure Homebrew's bin directory is on PATH and re-run this script.",
    "deps": "Install Python dependencies with 'python -m scripts.deps --install-optional'.",
    "profile": (
        "Add 'eval $(brew shellenv)' and 'source \"$HOME/.cargo/env\"' to your shell profile"
        " so the tools are available in new shells."
    ),
}

# mapping of automatic fix functions for each setup step.  Each function should
# attempt to resolve the issue and is followed by a retry of the original step
# in :func:`prepare_macos_env`.
AUTO_FIXES: dict[str, Callable[[], None]] = {
    "xcode": lambda: _run(["xcode-select", "--install"], check=False),
    "homebrew": lambda: _run(
        [
            "/bin/bash",
            "-c",
            "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)",
        ]
    ),
    "brew_packages": lambda: _run_with_retry(
        [
            "brew",
            "install",
            PY_FORMULA,
            "rustup-init",
            "pkg-config",
            "cmake",
            "protobuf",
        ]
    ),
    "rustup": lambda: _run(["rustup-init", "-y"]),
    "pip_torch": upgrade_pip_and_torch,
    "verify_tools": apply_brew_env,
    "deps": install_deps,
    "profile": ensure_profile,
}


def prepare_macos_env(non_interactive: bool = True) -> dict[str, object]:
    """Ensure core macOS development tools are installed.

    Returns a report dictionary describing the status of each setup step and
    applies the Homebrew environment variables.  Each step is logged via
    :func:`solhunter_zero.logging_utils.log_startup`.
    """

    if parse_bool_env("SOLHUNTER_OFFLINE", False) or parse_bool_env("SOLHUNTER_SKIP_CONNECTIVITY", False):
        log_startup("macOS setup: skipping network check (offline mode enabled)")
    else:
        ensure_network()

    report: dict[str, object] = {"steps": {}, "success": True}

    steps: list[tuple[str, Callable[[], None]]] = [
        ("xcode", lambda: ensure_xcode(non_interactive)),
        ("homebrew", ensure_homebrew),
        ("brew_packages", install_brew_packages),
        ("rustup", ensure_rustup),
        ("pip_torch", upgrade_pip_and_torch),
        ("verify_tools", verify_tools),
        ("deps", install_deps),
        ("profile", ensure_profile),
    ]

    for idx, (name, func) in enumerate(steps):
        try:
            func()
        except Exception as exc:
            # Attempt automatic fix
            fix = AUTO_FIXES.get(name)
            if fix:
                print(f"Step '{name}' failed: {exc}. Attempting automatic fix...")
                try:
                    fix()
                    func()
                except Exception as exc2:
                    print(f"Automatic fix for {name} failed: {exc2}")
                    report["steps"][name] = {
                        "status": "error",
                        "message": str(exc2),
                    }
                    report["success"] = False
                    for n, _ in steps[idx + 1 :]:
                        report["steps"][n] = {"status": "skipped"}
                    break
                else:
                    print(f"Automatic fix for {name} succeeded")
                    report["steps"][name] = {"status": "ok", "fixed": True}
                    continue
            # No fix available or fix not attempted
            report["steps"][name] = {"status": "error", "message": str(exc)}
            report["success"] = False
            for n, _ in steps[idx + 1 :]:
                report["steps"][n] = {"status": "skipped"}
            break
        else:
            report["steps"][name] = {"status": "ok"}

    if report.get("success"):
        MAC_SETUP_MARKER.parent.mkdir(parents=True, exist_ok=True)
        MAC_SETUP_MARKER.write_text("ok")

    apply_brew_env()
    for step, info in report["steps"].items():
        status = info.get("status")
        message = info.get("message", "")
        if message:
            log_startup(f"mac setup {step}: {status} - {message}")
        else:
            log_startup(f"mac setup {step}: {status}")
    if report.get("success"):
        log_startup("mac setup completed successfully")
    else:
        log_startup("mac setup failed")

    _write_report(report)
    return report


def ensure_tools(*, non_interactive: bool = True) -> dict[str, object]:
    """Ensure essential macOS development tools are present.

    On Apple Silicon Macs this checks for Homebrew, Xcode command line tools,
    Python and rustup.  If any are missing ``prepare_macos_env`` is
    invoked to install them.  A report dictionary is returned mirroring
    ``prepare_macos_env``'s output and listing any missing components.
    """

    if platform.system() != "Darwin" or platform.machine() != "arm64":
        report = {"steps": {}, "success": True, "missing": []}
        _write_report(report)
        return report

    missing_tools = _detect_missing_tools()
    if missing_tools:
        msg = (
            "Missing macOS tools: "
            + ", ".join(missing_tools)
            + ". Running mac setup..."
        )
        log_startup(msg)
        print(msg)
        report = prepare_macos_env(non_interactive=non_interactive)
        for step, info in report["steps"].items():
            msg = info.get("message", "")
            if msg:
                step_msg = f"{step}: {info['status']} - {msg}"
            else:
                step_msg = f"{step}: {info['status']}"
            log_startup(step_msg)
            print(step_msg)
        report["missing"] = _detect_missing_tools()
        if report["missing"]:
            err_msg = (
                "macOS environment preparation failed; continuing without required tools"
            )
            log_startup(err_msg)
            print(err_msg, file=sys.stderr)
            for step, info in report["steps"].items():
                if info.get("status") == "error":
                    fix = MANUAL_FIXES.get(step)
                    if fix:
                        fix_msg = f"Manual fix for {step}: {fix}"
                        log_startup(fix_msg)
                        print(fix_msg)
    else:
        report = {"steps": {}, "success": True, "missing": []}

    if report.get("success") and not report["missing"]:
        TOOLS_OK_MARKER.parent.mkdir(parents=True, exist_ok=True)
        TOOLS_OK_MARKER.write_text("ok")

    _write_report(report)
    return report

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--non-interactive", action="store_true")
    args = parser.parse_args(argv)
    report = prepare_macos_env(args.non_interactive)
    for step, info in report["steps"].items():
        msg = info.get("message", "")
        if msg:
            print(f"{step}: {info['status']} - {msg}")
        else:
            print(f"{step}: {info['status']}")
    if not report.get("success"):
        raise SystemExit(1)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
