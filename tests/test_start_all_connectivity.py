from __future__ import annotations

from scripts import start_all


def test_connectivity_soak_disabled_for_zero_duration(monkeypatch):
    monkeypatch.setenv("CONNECTIVITY_SOAK_DURATION", "0")
    monkeypatch.delenv("SKIP_CONNECTIVITY_SOAK", raising=False)
    monkeypatch.setattr(start_all, "_SKIP_CONNECTIVITY_SOAK", False)

    constructed = False

    class DummyChecker:
        def __init__(self) -> None:
            nonlocal constructed
            constructed = True

        async def run_soak(self, *args, **kwargs):  # pragma: no cover - not expected
            raise AssertionError("run_soak should not be called when duration is zero")

    monkeypatch.setattr(start_all, "ConnectivityChecker", DummyChecker)

    result = start_all._connectivity_soak()

    assert result == {"disabled": True, "duration": 0.0}
    assert constructed is False
