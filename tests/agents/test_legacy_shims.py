"""Smoke tests for legacy agent shims."""

from __future__ import annotations


def test_agents_http_wrapper_exports_expected_helpers() -> None:
    import solhunter_zero.http as root_http
    import solhunter_zero.agents.http as shim

    assert shim.get_session is root_http.get_session
    assert shim.close_session is root_http.close_session
    assert shim.dumps is root_http.dumps
    assert shim.loads is root_http.loads
    assert shim.check_endpoint is root_http.check_endpoint


def test_agents_dynamic_limit_wrapper_exports_expected_helpers() -> None:
    import solhunter_zero.dynamic_limit as root_dynamic_limit
    import solhunter_zero.agents.dynamic_limit as shim

    assert shim.refresh_params is root_dynamic_limit.refresh_params
    assert shim._target_concurrency is root_dynamic_limit._target_concurrency
    assert shim._step_limit is root_dynamic_limit._step_limit
