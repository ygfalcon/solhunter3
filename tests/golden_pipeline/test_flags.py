import pytest

from solhunter_zero.golden_pipeline.flags import resolve_depth_flag


def test_depth_flag_defaults_enabled_for_live_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Golden depth extensions should stay enabled unless explicitly disabled."""

    for env_var in ("GOLDEN_DEPTH_ENABLED", "SOLHUNTER_MODE"):
        monkeypatch.delenv(env_var, raising=False)
    monkeypatch.setenv("MODE", "live")

    assert resolve_depth_flag({}) is True


def test_depth_flag_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("GOLDEN_DEPTH_ENABLED", "0")

    assert resolve_depth_flag({}) is False
