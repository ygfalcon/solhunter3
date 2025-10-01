from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from pathlib import Path

from .paths import ROOT
from .logging_utils import log_startup
from .console_utils import console_print, console_warning, console_error


def build_rust_component(
    name: str, cargo_path: Path, output: Path, *, target: str | None = None
) -> None:
    cmd = ["cargo", "build", "--manifest-path", str(cargo_path), "--release"]
    if target is not None:
        try:
            installed_targets = subprocess.check_output(
                ["rustup", "target", "list", "--installed"], text=True
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError("failed to verify rust targets") from exc
        if target not in installed_targets:
            subprocess.check_call(["rustup", "target", "add", target])
        cmd.extend(["--target", target])
    elif platform.system() == "Darwin" and platform.machine() == "arm64":
        try:
            installed_targets = subprocess.check_output(
                ["rustup", "target", "list", "--installed"], text=True
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError("failed to verify rust targets") from exc
        if "aarch64-apple-darwin" not in installed_targets:
            subprocess.check_call(["rustup", "target", "add", "aarch64-apple-darwin"])
        cmd.extend(["--target", "aarch64-apple-darwin"])

    subprocess.check_call(cmd)

    artifact = output.name
    target_dirs = [cargo_path.parent / "target", ROOT / "target"]
    candidates: list[Path] = []
    for base in target_dirs:
        candidates.append(base / "release" / artifact)
        if target is not None:
            candidates.append(base / target / "release" / artifact)
        elif platform.system() == "Darwin" and platform.machine() == "arm64":
            candidates.append(base / "aarch64-apple-darwin" / "release" / artifact)

    built = next((p for p in candidates if p.exists()), None)
    if built is None:
        paths = ", ".join(str(p) for p in candidates)
        raise RuntimeError(f"failed to build {name}: expected {artifact} in {paths}")

    if built.resolve() != output.resolve():
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(built, output)

    if not output.exists():
        raise RuntimeError(
            f"{name} build succeeded but {output} is missing. Please build manually."
        )

    if platform.system() == "Darwin":
        try:
            subprocess.check_call(["codesign", "--force", "--sign", "-", str(output)])
        except subprocess.CalledProcessError as exc:
            console_warning(
                f"failed to codesign {output}: {exc}. "
                "Please codesign the binary manually if required."
            )


def ensure_target(name: str) -> None:
    """Ensure build artifacts for *name* exist.

    Parameters
    ----------
    name:
        The build target to ensure. Supported values are ``"route_ffi"``,
        ``"depth_service"`` and ``"protos"``.
    """

    if name == "route_ffi":
        libname = (
            "libroute_ffi.dylib" if platform.system() == "Darwin" else "libroute_ffi.so"
        )
        libpath = ROOT / "solhunter_zero" / libname
        if libpath.exists():
            return
        try:
            build_rust_component(
                "route_ffi",
                ROOT / "route_ffi" / "Cargo.toml",
                libpath,
            )
        except Exception as exc:  # pragma: no cover - build errors are rare
            console_error(f"Failed to build route_ffi: {exc}")
            console_print(
                "To retry the build, run 'cargo build --manifest-path route_ffi/Cargo.toml --release' "
                "and re-run this program."
            )
            raise SystemExit(1)
        return

    if name == "depth_service":
        bin_path = ROOT / "target" / "release" / "depth_service"
        if bin_path.exists():
            return
        target = "aarch64-apple-darwin" if platform.system() == "Darwin" else None
        try:
            build_rust_component(
                "depth_service",
                ROOT / "depth_service" / "Cargo.toml",
                bin_path,
                target=target,
            )
        except Exception as exc:  # pragma: no cover - build errors are rare
            hint = ""
            if platform.system() == "Darwin":
                hint = " Hint: run 'scripts/mac_setup.py' to install macOS build tools."
            console_error(f"Failed to build depth_service: {exc}.{hint}")
            console_print(
                "You can retry by running 'cargo build --manifest-path depth_service/Cargo.toml --release' "
                "and then re-running this program."
            )
            raise SystemExit(1)
        return

    if name == "protos":
        script = ROOT / "scripts" / "check_protos.py"
        result = subprocess.run([sys.executable, str(script)], cwd=ROOT)
        if result.returncode == 0:
            return

        console_print("event_pb2.py is stale. Regenerating...")
        try:
            regen = ROOT / "scripts" / "gen_proto.py"
            subprocess.check_call([sys.executable, str(regen)], cwd=ROOT)
        except subprocess.CalledProcessError as exc:  # pragma: no cover - rare
            console_error(f"Failed to regenerate event_pb2.py: {exc}")
            log_startup(f"Failed to regenerate event_pb2.py: {exc}")
            raise SystemExit(1)

        msg = "Regenerated event_pb2.py from proto/event.proto"
        console_print(msg)
        log_startup(msg)
        return

    raise ValueError(f"unknown target: {name}")
