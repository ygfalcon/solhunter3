#!/usr/bin/env python3
"""Generate Python protobuf bindings for event.proto."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        "proto",
        "--python_out=solhunter_zero",
        "proto/event.proto",
    ]
    try:
        subprocess.check_call(cmd, cwd=ROOT)
    except subprocess.CalledProcessError as exc:  # pragma: no cover - external tool failure
        print(f"Failed to generate event_pb2.py: {exc}")
        return exc.returncode
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
