import os

from solhunter_zero import env


def test_load_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FOO=bar\nBAZ=qux\n# comment\n")

    monkeypatch.delenv("FOO", raising=False)
    monkeypatch.delenv("BAZ", raising=False)

    env.load_env_file(env_file)
    assert os.environ["FOO"] == "bar"
    assert os.environ["BAZ"] == "qux"

    monkeypatch.setenv("FOO", "orig")
    monkeypatch.setenv("BAZ", "orig2")
    env.load_env_file(env_file)
    assert os.environ["FOO"] == "orig"
    assert os.environ["BAZ"] == "orig2"

    missing = tmp_path / "missing.env"
    env.load_env_file(missing)
    assert missing.exists()
    assert "SOLANA_RPC_URL" in missing.read_text()
