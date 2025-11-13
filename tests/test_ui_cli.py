import os
import socket
import subprocess
import sys


def test_ui_cli_reports_bind_failure() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        host, port = sock.getsockname()

        env = os.environ.copy()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "solhunter_zero.ui",
                "--host",
                host,
                "--port",
                str(port),
            ],
            capture_output=True,
            text=True,
            env=env,
        )

    assert result.returncode != 0
    stderr = result.stderr
    assert "Failed to start UI server" in stderr
    assert f"{host}:{port}" in stderr
