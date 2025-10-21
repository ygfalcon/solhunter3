import pytest

from solhunter_zero.feature_flags import FeatureFlags, emit_feature_flag_metrics


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    # Ensure tests start from a clean slate for the feature flag env vars.
    keys = [
        "MODE",
        "MICRO_MODE",
        "NEW_DAS_DISCOVERY",
        "ONCHAIN_USE_DAS",
        "ONCHAIN_DISCOVERY_PROVIDER",
        "EXIT_FEATURES_ON",
        "RL_WEIGHTS_DISABLED",
        "MINT_STREAM_ENABLE",
        "MEMPOOL_STREAM_ENABLE",
        "AMM_WATCH_ENABLE",
        "SEED_PUBLISH_ENABLE",
        "SEED_TOKENS",
        "SHADOW_EXECUTOR_ONLY",
        "PAPER_TRADING",
        "LIVE_TRADING_DISABLED",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def test_feature_flags_capture_stream_toggles(monkeypatch):
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("MICRO_MODE", "1")
    monkeypatch.setenv("NEW_DAS_DISCOVERY", "0")
    monkeypatch.setenv("ONCHAIN_DISCOVERY_PROVIDER", "das")
    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    monkeypatch.setenv("EXIT_FEATURES_ON", "1")
    monkeypatch.setenv("RL_WEIGHTS_DISABLED", "0")
    monkeypatch.setenv("MINT_STREAM_ENABLE", "1")
    monkeypatch.setenv("MEMPOOL_STREAM_ENABLE", "1")
    monkeypatch.setenv("AMM_WATCH_ENABLE", "1")
    monkeypatch.setenv("SEED_PUBLISH_ENABLE", "1")
    monkeypatch.setenv("SEED_TOKENS", "ABC,DEF")
    monkeypatch.setenv("SHADOW_EXECUTOR_ONLY", "1")
    monkeypatch.setenv("PAPER_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_DISABLED", "0")

    flags = FeatureFlags.from_env()
    assert flags.mode == "live"
    assert flags.micro_mode is True
    assert flags.das_enabled is True
    assert flags.mint_stream_enabled is True
    assert flags.mempool_stream_enabled is True
    assert flags.amm_watch_enabled is True
    assert flags.seeded_tokens_enabled is True
    assert flags.rl_shadow_mode is True
    assert flags.paper_trading is False
    assert flags.live_trading_disabled is False

    metrics = flags.as_metrics()
    assert metrics["mode"] == 1.0
    assert metrics["mint_stream_enabled"] == 1.0
    assert metrics["seeded_tokens_enabled"] == 1.0
    assert metrics["paper_trading"] == 0.0

    ui_flags = flags.for_ui()
    assert ui_flags["mode"] == "live"
    assert ui_flags["amm_watch_enabled"] is True
    assert ui_flags["live_trading_disabled"] is False

    published: dict[str, float] = {}

    def capture(metric: str, value: float) -> None:
        published[metric] = value

    emit_feature_flag_metrics(capture)

    assert published["feature_flag_mint_stream_enabled"] == 1.0
    assert published["feature_flag_live_trading_disabled"] == 0.0
