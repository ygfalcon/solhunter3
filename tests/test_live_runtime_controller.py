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


def test_emit_runtime_manifest_rediss_requires_db_one(monkeypatch):
    monkeypatch.setenv("BROKER_CHANNEL", "solhunter-events-v3")
    monkeypatch.setenv("REDIS_URL", "rediss://cache.example:6379/2")

    with pytest.raises(SystemExit):
        _emit_runtime_manifest()
