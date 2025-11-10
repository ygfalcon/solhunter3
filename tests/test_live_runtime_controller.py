import logging
import os

import pytest

from scripts.live_runtime_controller import _emit_runtime_manifest


REDIS_KEYS = [
    "REDIS_URL",
    "MINT_STREAM_REDIS_URL",
    "MEMPOOL_STREAM_REDIS_URL",
    "AMM_WATCH_REDIS_URL",
]


@pytest.mark.parametrize(
    "url",
    [
        "redis://cache.example:6379/1",
        "rediss://cache.example:6380/1",
    ],
)
def test_emit_runtime_manifest_supports_redis_schemes(monkeypatch, url):
    monkeypatch.setenv("BROKER_CHANNEL", "solhunter-events-v3")
    for key in REDIS_KEYS:
        monkeypatch.setenv(key, url)

    _emit_runtime_manifest()

    for key in REDIS_KEYS:
        assert os.environ[key] == url


def test_emit_runtime_manifest_preserves_credentials_and_query(monkeypatch, caplog):
    credentialed = "redis://user:pass@cache.example:6379/1?ssl_cert_reqs=none"
    monkeypatch.setenv("BROKER_CHANNEL", "solhunter-events-v3")
    for key in REDIS_KEYS:
        monkeypatch.setenv(key, credentialed)

    caplog.set_level(logging.INFO)

    _emit_runtime_manifest()

    for key in REDIS_KEYS:
        assert os.environ[key] == credentialed

    manifest_logs = [record.message for record in caplog.records if record.message.startswith("RUNTIME_MANIFEST")]
    assert manifest_logs, "Expected runtime manifest to be logged"
    manifest = manifest_logs[-1]
    assert f"redis={credentialed}" in manifest
    assert f"mint_stream={credentialed}" in manifest
    assert f"mempool={credentialed}" in manifest
    assert f"amm_watch={credentialed}" in manifest


def test_emit_runtime_manifest_rediss_requires_db_one(monkeypatch):
    monkeypatch.setenv("BROKER_CHANNEL", "solhunter-events-v3")
    monkeypatch.setenv("REDIS_URL", "rediss://cache.example:6379/2")

    with pytest.raises(SystemExit):
        _emit_runtime_manifest()
