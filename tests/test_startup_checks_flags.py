from types import SimpleNamespace

import pytest

from solhunter_zero import startup_checks


@pytest.mark.parametrize(
    "offline, skip_rpc, skip_endpoints, expect_rpc, expect_endpoints",
    [
        (True, False, False, False, False),
        (False, True, False, False, True),
        (False, False, True, True, False),
        (False, False, False, True, True),
    ],
)
def test_perform_checks_respects_flags(
    monkeypatch,
    offline,
    skip_rpc,
    skip_endpoints,
    expect_rpc,
    expect_endpoints,
):
    rpc_called = []
    endpoints_called = []

    def fake_rpc(*, warn_only):
        rpc_called.append(warn_only)

    def fake_endpoints(_cfg: dict[str, object] | None = None):
        endpoints_called.append(True)

    monkeypatch.setattr(startup_checks, "ensure_rpc", fake_rpc)
    monkeypatch.setattr(startup_checks, "ensure_endpoints", fake_endpoints)

    args = SimpleNamespace(
        offline=offline,
        skip_rpc_check=skip_rpc,
        skip_endpoint_check=skip_endpoints,
    )

    startup_checks.perform_checks(args, [])

    assert bool(rpc_called) is expect_rpc
    assert bool(endpoints_called) is expect_endpoints
    if rpc_called:
        assert rpc_called == [True]


def test_perform_checks_skip_network_calls(monkeypatch):
    """Offline mode should not attempt any network access."""

    def fail_rpc(*, warn_only):  # pragma: no cover - ensures we would fail if called
        raise AssertionError("RPC check should be skipped")

    def fail_endpoints(_cfg: dict[str, object] | None = None):  # pragma: no cover
        raise AssertionError("Endpoint check should be skipped")

    monkeypatch.setattr(startup_checks, "ensure_rpc", fail_rpc)
    monkeypatch.setattr(startup_checks, "ensure_endpoints", fail_endpoints)

    args = SimpleNamespace(offline=True, skip_rpc_check=False, skip_endpoint_check=False)

    startup_checks.perform_checks(args, [])
