import pathlib
import subprocess
import sys


def test_check_protos_detects_stale_file():
    root = pathlib.Path(__file__).resolve().parents[1]
    script = root / "scripts" / "check_protos.py"
    event_pb = root / "solhunter_zero" / "event_pb2.py"

    original = event_pb.read_text()

    try:
        # First ensure the committed file passes
        subprocess.run([sys.executable, str(script)], check=True, cwd=root)

        # Modify file to make it stale
        event_pb.write_text(original + "\n# test modification")

        # Now the check should fail
        result = subprocess.run([sys.executable, str(script)], cwd=root)
        assert result.returncode != 0, "check_protos should fail when event_pb2.py is stale"
    finally:
        event_pb.write_text(original)
