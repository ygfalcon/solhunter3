"""Tests for TTL cache pruning behaviour."""

from __future__ import annotations

import math

import pytest

from solhunter_zero.golden_pipeline import utils


def test_ttl_cache_prunes_expired_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    cache = utils.TTLCache()
    current_time = 0.0

    def _fake_now() -> float:
        return current_time

    monkeypatch.setattr(utils, "now_ts", _fake_now)

    ttl = 1.0
    step = 0.6
    observed_sizes: list[int] = []

    for i in range(100):
        current_time = i * step
        cache.add(f"token-{i}", ttl)
        observed_sizes.append(len(cache._data))  # type: ignore[attr-defined]

    bound = int(math.ceil(ttl / step)) + 2
    assert max(observed_sizes) <= bound
    assert len(cache._data) <= bound  # type: ignore[attr-defined]
