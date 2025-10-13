import json
import os
from pathlib import Path

import pytest

from solhunter_zero.production.env import (
    Provider,
    assert_providers_ok,
    detect_placeholder,
    format_configured_providers,
    load_env_file,
    validate_providers,
    write_env_manifest,
)


def test_detect_placeholder_patterns():
    assert detect_placeholder("REDACTED_VALUE")
    assert detect_placeholder("YOUR_SECRET_KEY")
    assert detect_placeholder("test_key")
    assert detect_placeholder("") == "empty"
    assert detect_placeholder("actual-value") is None


def test_validate_providers_success(monkeypatch):
    monkeypatch.setenv("SOLANA_RPC_URL", "https://solhunter.dev/rpc")
    monkeypatch.setenv("SOLANA_WS_URL", "wss://solhunter.dev/ws")
    monkeypatch.setenv("HELIUS_API_KEY", "abc123")
    providers = [
        Provider("Solana", ("SOLANA_RPC_URL", "SOLANA_WS_URL")),
        Provider("Helius", ("HELIUS_API_KEY",)),
    ]
    missing, placeholders = validate_providers(providers)
    assert not missing
    assert not placeholders
    assert "Solana" in format_configured_providers(providers)


def test_validate_providers_failure(monkeypatch):
    monkeypatch.delenv("HELIUS_API_KEY", raising=False)
    providers = [Provider("Helius", ("HELIUS_API_KEY",))]
    missing, placeholders = validate_providers(providers)
    assert missing and not placeholders
    with pytest.raises(RuntimeError):
        assert_providers_ok(providers)


def test_write_env_manifest(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("SOLANA_RPC_URL", "https://solhunter.dev/rpc")
    provider = Provider("Solana", ("SOLANA_RPC_URL",))
    manifest_path = tmp_path / "manifest.json"
    result = write_env_manifest(manifest_path, [provider], source_map={"SOLANA_RPC_URL": "env"})
    payload = json.loads(result.read_text())
    assert payload["entries"][0]["source"] == "env"
    assert payload["entries"][0]["configured"] is True


def test_load_env_file(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("SAMPLE_KEY=value\n")
    monkeypatch.delenv("SAMPLE_KEY", raising=False)
    loaded = load_env_file(env_file, overwrite=True)
    assert loaded == {"SAMPLE_KEY": "value"}
    assert os.environ["SAMPLE_KEY"] == "value"
