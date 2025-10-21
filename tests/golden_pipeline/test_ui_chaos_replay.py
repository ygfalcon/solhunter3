pytest_plugins = ["tests.golden_pipeline.synth_seed"]


def test_ui_websocket_chaos_replay(runtime, chaos_replay):
    result = chaos_replay(mint_count=64, depth_updates=96)
    summary = result["summary"]
    channels = result["channels"]

    assert result["payloads"] >= 5000
    assert summary["golden"]["count"] == result["mint_count"]

    lag = summary.get("lag", {})
    assert lag, "lag snapshot missing"

    max_bus_delay = lag.get("bus_ms")
    assert max_bus_delay is not None and max_bus_delay < 2000.0

    for key in ("depth_ms", "ohlcv_ms", "golden_ms", "suggestion_ms", "decision_ms"):
        value = lag.get(key)
        if value is None:
            continue
        assert value < 2000.0

    backpressure = summary.get("backpressure", {}).get("websockets", {})
    assert backpressure, "websocket backpressure metrics missing"

    for channel, stats in backpressure.items():
        channel_metrics = channels.get(channel, {})
        assert stats == channel_metrics
        queue_max = stats.get("queue_max")
        queue_depth = stats.get("queue_depth")
        queue_high = stats.get("queue_high")
        drops = stats.get("drops")
        assert isinstance(queue_max, int) and queue_max >= 1000
        assert isinstance(queue_depth, int)
        assert isinstance(queue_high, int)
        assert isinstance(drops, int)
        assert 0 <= queue_depth <= queue_max
        assert 0 <= queue_high <= queue_max
        assert drops <= 1
        if queue_high:
            assert queue_high <= max(1, queue_max * 0.95)
        recent_codes = stats.get("recent_close_codes", [])
        assert all(code in (None, 1000) for code in recent_codes)

    events_metrics = backpressure.get("events", {})
    assert events_metrics.get("drops", 1) == 0
    assert events_metrics.get("queue_high", 0) <= events_metrics.get("queue_max", 1)
