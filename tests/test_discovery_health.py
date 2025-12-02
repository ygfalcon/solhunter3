import scripts.discovery_health as dh


def _health(cursor_age: float, rate: float, samples: int = 5) -> dh.DiscoveryHealth:
    return dh.DiscoveryHealth(
        cursor_ttl=dh.DEFAULT_CURSOR_TTL,
        cursor_age=cursor_age,
        latest_ts_ms=1700000000000,
        message_rate_per_min=rate,
        samples=samples,
    )


def test_slow_discovery_blocks_launch(monkeypatch):
    slow_health = _health(cursor_age=dh.DEFAULT_MAX_CURSOR_AGE + 10, rate=0.1)
    async def _fake_gather(*_a, **_k):
        return slow_health

    monkeypatch.setattr(dh, "_gather_health", _fake_gather)

    result = dh.main(["--redis-url", "redis://localhost"])
    assert result == 1


def test_steady_discovery_passes(monkeypatch):
    healthy = _health(cursor_age=30.0, rate=10.0, samples=20)
    async def _fake_gather(*_a, **_k):
        return healthy

    monkeypatch.setattr(dh, "_gather_health", _fake_gather)

    result = dh.main(["--redis-url", "redis://localhost"])
    assert result == 0
