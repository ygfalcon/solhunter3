import logging
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO

from .paths import ROOT
from .cache_paths import CARGO_MARKER
logger = logging.getLogger(__name__)


def _stream_stderr(pipe: IO[bytes]) -> None:
    for line in iter(pipe.readline, b""):
        sys.stderr.buffer.write(line)
    pipe.close()


def _ensure_cargo() -> None:
    """Ensure the Rust toolchain is installed via rustup."""
    cargo_bin = Path.home() / ".cargo" / "bin"
    os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"
    if shutil.which("cargo") is not None:
        return
    if CARGO_MARKER.exists():
        raise RuntimeError(
            "Rust toolchain previously installed but 'cargo' was not found. "
            "Ensure ~/.cargo/bin is in your PATH or remove the cache marker and rerun."
        )
    CARGO_MARKER.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Installing Rust toolchain via rustup...")
    try:
        subprocess.run(["rustup-init", "-y"], check=True)
    except FileNotFoundError as exc:  # pragma: no cover - rustup missing is rare
        raise RuntimeError(
            "rustup-init not found. Please install rustup from https://rustup.rs/."
        ) from exc
    CARGO_MARKER.touch()
    if shutil.which("cargo") is None:
        raise RuntimeError("cargo installation failed; ensure ~/.cargo/bin is in PATH")


def start_depth_service(
    cfg_path: str | None = None, *, stream_stderr: bool = False
) -> subprocess.Popen:
    """Start the depth_service binary, building it if needed."""
    depth_bin = ROOT / "target" / "release" / "depth_service"
    if not depth_bin.exists() or not os.access(depth_bin, os.X_OK):
        _ensure_cargo()
        logger.info("depth_service binary not found, building with cargo...")
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(ROOT / "depth_service" / "Cargo.toml"),
                "--release",
            ],
            check=True,
        )
        if not depth_bin.exists() or not os.access(depth_bin, os.X_OK):
            raise RuntimeError(
                "depth_service binary missing or not executable after build. "
                "Please run 'cargo build --manifest-path "
                "depth_service/Cargo.toml --release'."
            )
    cmd = [str(depth_bin)]
    if cfg_path:
        cmd += ["--config", cfg_path]
    keypair_path = os.getenv("KEYPAIR_PATH") or os.getenv("SOLANA_KEYPAIR")
    if keypair_path:
        cmd += ["--keypair", keypair_path]
    env = os.environ.copy()
    if "DEPTH_SERVICE_VERSION" not in env:
        try:
            result = subprocess.run(
                [str(depth_bin), "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            version_text = None
        else:
            stdout = (result.stdout or "").strip()
            stderr = (result.stderr or "").strip()
            version_text = stdout or stderr or None
        if version_text:
            env["DEPTH_SERVICE_VERSION"] = version_text
    if "DEPTH_SERVICE_RPC_URL" not in env:
        rpc_url = os.getenv("DEPTH_SERVICE_RPC_URL") or os.getenv("SOLANA_RPC_URL")
        if rpc_url:
            env["DEPTH_SERVICE_RPC_URL"] = rpc_url
    stderr = subprocess.PIPE if stream_stderr else None
    proc = subprocess.Popen(cmd, env=env, stderr=stderr)
    if stream_stderr and proc.stderr is not None:
        threading.Thread(
            target=_stream_stderr, args=(proc.stderr,), daemon=True
        ).start()
    return proc


def start_rl_daemon(*, stream_stderr: bool = True) -> subprocess.Popen:
    """Start the reinforcement learning daemon.

    Parameters
    ----------
    stream_stderr:
        When ``True`` (the default) the RL daemon's standard error is piped
        through the parent process so that any exceptions are visible in the
        main console.
    """

    env = os.environ.copy()
    # Ensure the repository root is importable so the child process can import
    # the local solhunter_zero package even when invoked from the scripts/ dir.
    prev_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{ROOT}{os.pathsep}{prev_pp}" if prev_pp else str(ROOT)
    cmd = [sys.executable, "scripts/run_rl_daemon.py"]
    stderr = subprocess.PIPE if stream_stderr else None
    proc = subprocess.Popen(cmd, env=env, stderr=stderr)
    if stream_stderr and proc.stderr is not None:
        threading.Thread(
            target=_stream_stderr, args=(proc.stderr,), daemon=True
        ).start()
    return proc


def wait_for_depth_ws(
    addr: str,
    port: int,
    deadline: float,
    depth_proc: subprocess.Popen | None = None,
) -> None:
    """Wait for the depth_service websocket to accept connections."""
    while True:
        if depth_proc is not None and depth_proc.poll() is not None:
            raise RuntimeError(
                f"depth_service exited with code {depth_proc.returncode}"
            )
        try:
            with socket.create_connection((addr, port), timeout=1):
                break
        except OSError:
            if time.monotonic() > deadline:
                raise TimeoutError("depth_service websocket timed out")
            time.sleep(0.1)
