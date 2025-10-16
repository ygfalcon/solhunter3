from __future__ import annotations

import argparse
import importlib
import logging
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from typing import Sequence

from .cache_paths import MPS_SENTINEL

logger = logging.getLogger(__name__)

METAL_EXTRA_INDEX = [
    "--extra-index-url",
    "https://download.pytorch.org/whl/metal",
]

INSTALL_TIMEOUT = 600

DEFAULT_TORCH_METAL_VERSION = "2.8.0"
DEFAULT_TORCHVISION_METAL_VERSION = "0.23.0"


def load_torch_metal_versions() -> tuple[str, str]:
    """Return compatible ``torch`` and ``torchvision`` Metal versions.

    The versions are read from the ``TORCH_METAL_VERSION`` and
    ``TORCHVISION_METAL_VERSION`` environment variables or from the ``[torch]``
    section of ``config.toml``.  When neither source provides values the
    versions are resolved dynamically and a warning is emitted.
    """

    torch_ver = os.getenv("TORCH_METAL_VERSION")
    vision_ver = os.getenv("TORCHVISION_METAL_VERSION")
    if not (torch_ver and vision_ver):
        try:
            from .config import load_config

            cfg = load_config()
            torch_cfg = cfg.get("torch", {})
            torch_ver = torch_ver or torch_cfg.get("torch_metal_version")
            vision_ver = vision_ver or torch_cfg.get("torchvision_metal_version")
        except Exception:
            logger.exception("Failed to load torch Metal versions from config")

    if not (torch_ver and vision_ver):
        try:
            from .macos_setup import _resolve_metal_versions

            torch_ver, vision_ver = _resolve_metal_versions()
            logger.warning(
                "Torch Metal versions not specified; using resolved versions %s/%s",
                torch_ver,
                vision_ver,
            )
        except Exception:  # pragma: no cover - network failure fallback
            torch_ver = DEFAULT_TORCH_METAL_VERSION
            vision_ver = DEFAULT_TORCHVISION_METAL_VERSION
            logger.warning(
                "Torch Metal versions not specified; falling back to defaults %s/%s",
                torch_ver,
                vision_ver,
            )

    return torch_ver, vision_ver


TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION = load_torch_metal_versions()


def _read_sentinel_versions() -> tuple[str, str] | None:
    """Return the torch and torchvision versions stored in the sentinel file.

    The helper returns ``None`` when the sentinel does not exist or is malformed
    so callers can decide whether it represents a version mismatch.
    """

    if not MPS_SENTINEL.exists():
        return None
    try:
        torch_ver, vision_ver = MPS_SENTINEL.read_text().splitlines()[:2]
    except Exception:
        return None
    return torch_ver, vision_ver


def _sentinel_matches() -> bool:
    versions = _read_sentinel_versions()
    return versions == (TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION)


def _write_sentinel() -> None:
    MPS_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
    MPS_SENTINEL.write_text(f"{TORCH_METAL_VERSION}\n{TORCHVISION_METAL_VERSION}\n")


try:  # pragma: no cover - optional dependency
    import torch
except Exception:  # pragma: no cover - torch is optional at runtime
    torch = None  # type: ignore


@dataclass
class InstallStatus:
    """Result of a subprocess installation command."""

    success: bool
    message: str


def _run_with_timeout(cmd: Sequence[str], timeout: int = 600) -> InstallStatus:
    """Run *cmd* with a timeout returning :class:`InstallStatus`.

    Parameters
    ----------
    cmd:
        Command to execute.
    timeout:
        Maximum seconds to wait before aborting.
    """

    try:
        subprocess.run(cmd, check=True, timeout=timeout)
        return InstallStatus(True, "")
    except subprocess.TimeoutExpired:
        return InstallStatus(False, f"Command timed out after {timeout} seconds")
    except subprocess.CalledProcessError as exc:
        return InstallStatus(False, f"Command failed with exit code {exc.returncode}")
    except Exception as exc:  # pragma: no cover - unexpected failure
        return InstallStatus(False, str(exc))


def ensure_torch_with_metal() -> None:
    """Ensure PyTorch with Metal backend is installed on macOS arm64.

    The helper installs specific ``torch`` and ``torchvision`` versions from the
    Metal wheels when running on Apple Silicon.  After installation the module is
    imported and the ``mps`` backend availability is verified.  A ``RuntimeError``
    is raised if the backend remains unavailable.  After a successful check a
    sentinel file is written to :data:`MPS_SENTINEL` so future runs can skip
    reinstallation attempts.
    """

    if platform.system() != "Darwin" or platform.machine() != "arm64":
        return

    logger = logging.getLogger(__name__)

    global torch
    try:
        needs_install = not (
            torch is not None
            and getattr(torch.backends, "mps", None)
            and torch.backends.mps.is_available()
        )
    except Exception:
        needs_install = True

    if needs_install:
        cmd = [
            sys.executable,
            "-m",
            "pip",
            "install",
            f"torch=={TORCH_METAL_VERSION}",
            f"torchvision=={TORCHVISION_METAL_VERSION}",
            *METAL_EXTRA_INDEX,
        ]
        install_cmd = " ".join(cmd)
        status = _run_with_timeout(cmd, timeout=INSTALL_TIMEOUT)
        if not status.success:
            logger.error("PyTorch installation failed: %s", status.message)
            logger.error("Install manually with: %s", install_cmd)
            raise RuntimeError("Failed to install MPS-enabled PyTorch")

        importlib.invalidate_caches()
        torch = importlib.import_module("torch")

        if not getattr(torch.backends, "mps", None) or not torch.backends.mps.is_available():
            logger.warning("MPS backend not available; attempting to reinstall Metal wheel")
            reinstall_cmd = [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--force-reinstall",
                f"torch=={TORCH_METAL_VERSION}",
                f"torchvision=={TORCHVISION_METAL_VERSION}",
                *METAL_EXTRA_INDEX,
            ]
            reinstall_cmd_str = " ".join(reinstall_cmd)
            status = _run_with_timeout(reinstall_cmd, timeout=INSTALL_TIMEOUT)
            if not status.success:
                logger.error("PyTorch reinstallation failed: %s", status.message)
                logger.error("Install manually with: %s", reinstall_cmd_str)
                raise RuntimeError("Failed to reinstall MPS-enabled PyTorch")
            importlib.invalidate_caches()
            torch = importlib.reload(torch)
            if not getattr(torch.backends, "mps", None) or not torch.backends.mps.is_available():
                logger.error(
                    "MPS backend still not available after installation. Install manually with: %s",
                    reinstall_cmd_str,
                )
                raise RuntimeError(
                    "MPS backend still not available. Install manually with: pip install "
                    f"torch=={TORCH_METAL_VERSION} torchvision=={TORCHVISION_METAL_VERSION} "
                    + " ".join(METAL_EXTRA_INDEX),
                )

    if not getattr(torch.backends, "mps", None) or not torch.backends.mps.is_available():
        raise RuntimeError("MPS backend still not available after installation attempts")

    _write_sentinel()



def detect_gpu(_attempt_install: bool = True) -> bool:
    """Return ``True`` when a supported GPU backend is available."""

    if torch is None:
        logging.getLogger(__name__).warning(
            "PyTorch is not installed; GPU unavailable",
        )
        return False
    try:
        system = platform.system()
        if system == "Darwin":
            machine = platform.machine()
            if machine == "x86_64":
                logging.getLogger(__name__).warning(
                    "Running under Rosetta (x86_64); GPU unavailable",
                )
                return False
            install_hint = (
                "Install with: pip install "
                f"torch=={TORCH_METAL_VERSION} torchvision=={TORCHVISION_METAL_VERSION} "
                "--extra-index-url https://download.pytorch.org/whl/metal"
            )
            if not getattr(torch.backends, "mps", None):
                logging.getLogger(__name__).warning(
                    "MPS backend not present; GPU unavailable. %s", install_hint
                )
                return False
            if not torch.backends.mps.is_built():
                logging.getLogger(__name__).warning(
                    "MPS backend not built; GPU unavailable. %s", install_hint
                )
                return False
            if "PYTORCH_ENABLE_MPS_FALLBACK" not in os.environ:
                os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
            elif os.environ.get("PYTORCH_ENABLE_MPS_FALLBACK") != "1":
                logging.getLogger(__name__).warning(
                    "PYTORCH_ENABLE_MPS_FALLBACK is not set to '1'; GPU unavailable",
                )
                os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
            if not torch.backends.mps.is_available():
                logging.getLogger(__name__).warning(
                    "MPS backend not available; GPU unavailable. %s", install_hint
                )
                return False
            try:
                torch.ones(1, device="mps").cpu()
            except Exception:
                logging.getLogger(__name__).exception(
                    "Tensor operation failed on mps backend",
                )
                return False
            return True
        if not torch.cuda.is_available():
            logging.getLogger(__name__).warning("CUDA backend not available")
            return False
        try:
            torch.ones(1, device="cuda").cpu()
        except Exception:
            logging.getLogger(__name__).exception(
                "Tensor operation failed on cuda backend",
            )
            return False
        return True
    except Exception:
        logging.getLogger(__name__).exception("Exception during GPU detection")
        return False

def get_gpu_backend() -> str | None:
    """Return the default GPU backend name if available."""

    if torch is not None:
        try:  # pragma: no cover - optional dependency
            mps_built = hasattr(torch, "backends") and hasattr(torch.backends, "mps")
            mps_available = mps_built and torch.backends.mps.is_available()
            if mps_built and os.environ.get("PYTORCH_ENABLE_MPS_FALLBACK") != "1":
                logging.getLogger(__name__).warning(
                    "MPS support detected but PYTORCH_ENABLE_MPS_FALLBACK is not set to '1'. "
                    "Export PYTORCH_ENABLE_MPS_FALLBACK=1 to enable CPU fallback for unsupported ops."
                )
            if mps_built:
                os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
            if torch.cuda.is_available() or mps_available:
                return "torch"
        except Exception:
            logging.getLogger(__name__).warning(
                "GPU backend probe failed", exc_info=True
            )
    try:  # pragma: no cover - optional dependency
        import cupy as cp  # type: ignore
    except ModuleNotFoundError:
        logging.getLogger(__name__).debug("CuPy not available; skipping GPU backend probe")
    except Exception:
        logging.getLogger(__name__).warning(
            "CuPy import failed during GPU probe", exc_info=True
        )
    else:
        try:
            if cp.cuda.runtime.getDeviceCount() > 0:
                return "cupy"
        except Exception:
            logging.getLogger(__name__).warning(
                "CuPy CUDA runtime probe failed", exc_info=True
            )
    return None


def get_default_device(device: str | "torch.device" | None = "auto") -> "torch.device":
    """Return a valid :class:`torch.device`.

    Parameters
    ----------
    device:
        Desired device identifier. If ``"auto"`` or ``None`` the function prefers
        Apple's Metal backend (``"mps"``) on macOS when available, then
        ``"cuda"`` or finally ``"cpu"``. If a non-CPU device is requested but
        unavailable the call falls back to the CPU.
    """

    if torch is None:
        raise RuntimeError("PyTorch is required for device selection")
    if device is None or (isinstance(device, str) and device == "auto"):
        if platform.system() == "Darwin" and torch.backends.mps.is_available():
            return torch.device("mps")
        if torch.cuda.is_available():
            return torch.device("cuda")
        if torch.backends.mps.is_available():
            return torch.device("mps")
        return torch.device("cpu")
    if isinstance(device, str) and device not in ("cpu", "mps") and not torch.cuda.is_available():
        return torch.device("cpu")
    return torch.device(device) if isinstance(device, str) else device


def ensure_gpu_env() -> dict[str, str]:
    """Configure environment variables for GPU execution.

    This helper is used internally by :func:`initialize_gpu`.  External callers
    should prefer :func:`initialize_gpu` to avoid diverging GPU configuration.

    If a GPU backend is available, ``TORCH_DEVICE`` is set to the preferred
    device (``"mps"`` on macOS with Apple Silicon or ``"cuda"`` elsewhere).
    When the Metal backend is used, ``PYTORCH_ENABLE_MPS_FALLBACK`` is set to
    ``"1"`` to allow CPU fallback for unsupported operations.  The function
    returns a dictionary of variables that were modified.
    """

    env: dict[str, str] = {}
    if torch is not None:
        try:
            system = platform.system()
            if system == "Darwin" and torch.backends.mps.is_available():
                env["TORCH_DEVICE"] = "mps"
                env["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
            elif torch.cuda.is_available():
                env["TORCH_DEVICE"] = "cuda"
        except Exception:  # pragma: no cover - best effort only
            logging.getLogger(__name__).exception("Exception during GPU env setup")

    # Default to CPU when no GPU backend is available
    env.setdefault("TORCH_DEVICE", "cpu")

    gpu_device = env["TORCH_DEVICE"]
    env["SOLHUNTER_GPU_AVAILABLE"] = "1" if gpu_device in ("mps", "cuda") else "0"
    env["SOLHUNTER_GPU_DEVICE"] = gpu_device
    for key, value in env.items():
        os.environ[key] = value
    return env


def verify_gpu() -> tuple[bool, str]:
    """Verify GPU availability and configure environment variables.

    The function ensures :func:`ensure_gpu_env` has been executed so
    ``SOLHUNTER_GPU_*`` variables are always populated.  If a GPU backend is
    detected the returned message indicates the device in use, otherwise a hint
    explaining how to enable GPU support is provided.
    """

    hint = (
        "No GPU backend detected. Install a Metal-enabled PyTorch build or run "
        "python -c 'from solhunter_zero.macos_setup import prepare_macos_env; prepare_macos_env()' to enable GPU support"
    )

    env_available = os.environ.get("SOLHUNTER_GPU_AVAILABLE")
    env_device = os.environ.get("SOLHUNTER_GPU_DEVICE")
    if env_available is not None:
        if env_available == "1":
            return True, f"Using GPU device: {env_device or 'unknown'}"
        return False, hint

    try:
        if not detect_gpu():
            ensure_gpu_env()
            return False, hint
        try:
            dev = get_default_device("auto")
        except Exception as exc:
            ensure_gpu_env()
            return False, f"GPU detected but unusable: {exc}"
        env = ensure_gpu_env()
        return True, f"Using GPU device: {env.get('TORCH_DEVICE', dev)}"
    except Exception as exc:  # pragma: no cover - defensive
        ensure_gpu_env()
        return False, str(exc)


_GPU_LOGGED = False


def initialize_gpu() -> dict[str, str]:
    """Ensure GPU environment variables are set and log once.

    This is the canonical entry point for GPU configuration; other modules
    should not call :func:`ensure_gpu_env` directly.  The helper delegates to
    :func:`ensure_gpu_env` to perform the actual environment configuration.  The
    configured variables are then appended to ``startup.log`` the first time the
    function is invoked.  Subsequent calls simply refresh the environment
    without producing additional log entries.  The mapping of environment
    variables is returned in all cases.
    """

    system = platform.system()
    machine = platform.machine()
    if system == "Darwin" and machine == "x86_64":
        raise RuntimeError(
            "Running under Rosetta (x86_64) is not supported; run via 'arch -arm64'"
        )
    if system == "Darwin" and machine == "arm64":
        sentinel_ok = _sentinel_matches()
        if not sentinel_ok:
            MPS_SENTINEL.unlink(missing_ok=True)
            try:
                ensure_torch_with_metal()
            except Exception as exc:  # pragma: no cover - defensive
                raise RuntimeError(
                    "Failed to configure Metal-enabled PyTorch. Run scripts/mac_setup.py"
                ) from exc
        else:
            try:
                needs_install = (
                    torch is None
                    or not getattr(torch.backends, "mps", None)
                    or not torch.backends.mps.is_available()
                )
                if needs_install:
                    if torch is None:
                        logging.getLogger(__name__).info(
                            "PyTorch missing despite sentinel %s; reinstalling", MPS_SENTINEL
                        )
                        ensure_torch_with_metal()
                        _write_sentinel()
                    else:
                        logging.getLogger(__name__).warning(
                            "MPS backend unavailable but sentinel %s exists; delete to retry",
                            MPS_SENTINEL,
                        )
            except Exception as exc:  # pragma: no cover - fail fast
                logging.getLogger(__name__).exception(
                    "Automatic PyTorch Metal setup failed"
                )
                raise RuntimeError(
                    "Failed to configure MPS-enabled PyTorch"
                ) from exc
    verify_gpu()
    env = ensure_gpu_env()
    if env.get("SOLHUNTER_GPU_AVAILABLE") == "0":
        logging.getLogger(__name__).info("GPU not detected; using CPU")
    global _GPU_LOGGED
    if not _GPU_LOGGED:
        from .logging_utils import log_startup

        for key, value in env.items():
            log_startup(f"{key}: {value}")
        _GPU_LOGGED = True
    return env


def _main() -> int:  # pragma: no cover - CLI helper
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-gpu", action="store_true", help="exit 0 if a GPU is available")
    parser.add_argument(
        "--setup-env",
        action="store_true",
        help="print export commands configuring GPU environment variables",
    )
    parser.add_argument(
        "--refresh-metal",
        action="store_true",
        help="delete the Metal install sentinel and reinstall",
    )
    args = parser.parse_args()
    if args.refresh_metal:
        MPS_SENTINEL.unlink(missing_ok=True)
        try:
            ensure_torch_with_metal()
        except Exception:
            logging.getLogger(__name__).exception(
                "Failed to refresh Metal installation",
            )
            return 1
    if args.check_gpu:
        return 0 if detect_gpu() else 1
    if args.setup_env:
        for k, v in ensure_gpu_env().items():
            print(f"export {k}={v}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(_main())
