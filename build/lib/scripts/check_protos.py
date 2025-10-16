#!/usr/bin/env python3
"""Verify generated protobuf modules are current."""

from __future__ import annotations

import difflib
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
PROTO = ROOT / "proto" / "event.proto"
COMMITTED = ROOT / "solhunter_zero" / "event_pb2.py"


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = pathlib.Path(tmpdir)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{PROTO.parent}",
                f"--python_out={tmp_path}",
                str(PROTO),
            ],
            check=True,
        )
        generated = tmp_path / "event_pb2.py"
        committed_lines = COMMITTED.read_text().splitlines(keepends=True)
        generated_lines = generated.read_text().splitlines(keepends=True)
        diff = list(
            difflib.unified_diff(
                committed_lines,
                generated_lines,
                fromfile=str(COMMITTED),
                tofile=str(generated),
            )
        )
        if diff:
            sys.stderr.writelines(diff)
            sys.stderr.write(
                "\nevent_pb2.py is stale. Regenerate with:\n"
            )
            sys.stderr.write("  python scripts/gen_proto.py\n")
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
