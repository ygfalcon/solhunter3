import os
import json
import subprocess
import sys
import pytest

try:
    from solders.keypair import Keypair
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pytest.skip("solders is required", allow_module_level=True)


def run_cli(args, env):
    return subprocess.run(
        [sys.executable, "-m", "solhunter_zero.wallet_cli", *args],
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )


def test_wallet_cli_save_list_select(tmp_path):
    env = os.environ.copy()
    env["KEYPAIR_DIR"] = str(tmp_path)
    kp = Keypair()
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    path = src_dir / "kp.json"
    path.write_text(json.dumps(list(kp.to_bytes())))

    run_cli(["save", "my", str(path)], env)
    result = run_cli(["list"], env)
    assert result.stdout.strip().splitlines() == ["my"]

    run_cli(["select", "my"], env)
    assert (tmp_path / "active").read_text().strip() == "my"
