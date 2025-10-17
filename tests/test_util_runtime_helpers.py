import asyncio
import importlib
import logging
import sys

import pytest

from solhunter_zero.util import (
    install_uvloop,
    parse_bool_env,
    run_coro,
    run_coro_sync,
    sanitize_priority_urls,
)


@pytest.mark.parametrize(
    "value,overrides,expected",
    [
        ("On", None, True),
        ("disable", None, False),
        ("custom", {"custom": True}, True),
    ],
)
def test_parse_bool_env_variants(monkeypatch, value, overrides, expected, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("FLAG", value)
    assert (
        parse_bool_env(
            "FLAG",
            overrides=overrides,
            extra_true=("enable",),
            extra_false=("disable",),
            log_unknown=True,
        )
        is expected
    )


def test_parse_bool_env_logs_unknown(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("FLAG", "maybe")
    assert parse_bool_env("FLAG", default=True, log_unknown=True) is True
    assert "Ignoring unknown boolean env FLAG='maybe'" in caplog.text


def test_install_uvloop_skips_on_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32", raising=False)
    assert install_uvloop() is False


def test_install_uvloop_idempotent(monkeypatch):
    class FakeUvloop:
        class EventLoopPolicy:  # pragma: no cover - simple stub
            def __call__(self):
                return object()

    calls: list[object] = []

    monkeypatch.setattr(sys.implementation, "name", "cpython", raising=False)
    monkeypatch.setattr(sys, "platform", "linux", raising=False)
    monkeypatch.delattr(asyncio, "uvloop_installed", raising=False)

    def fake_import(name):
        if name == "uvloop":
            return FakeUvloop
        raise ImportError

    def fake_set_event_loop_policy(policy):
        calls.append(policy)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    monkeypatch.setattr(asyncio, "set_event_loop_policy", fake_set_event_loop_policy)

    assert install_uvloop() is True
    assert calls, "uvloop should install policy"
    assert install_uvloop() is True
    assert len(calls) == 1


def test_run_coro_modes():
    async def coro():
        return 42

    assert run_coro(coro()) == 42

    async def outer():
        task = run_coro(coro(), name="test", shield=True)
        assert isinstance(task, asyncio.Task)
        assert task.get_name() == "test"
        return await task

    assert run_coro(outer()) == 42


def test_run_coro_logs_exceptions(caplog):
    async def boom():
        raise RuntimeError("boom")

    async def runner():
        task = run_coro(boom(), name="boom-task")
        try:
            await task
        except RuntimeError:
            pass

    caplog.set_level(logging.ERROR)
    run_coro(runner())
    assert "Background task boom-task failed" in caplog.text


def test_run_coro_sync(monkeypatch):
    async def produce():
        await asyncio.sleep(0)
        return 99

    result = run_coro_sync(produce())
    assert result == 99

    async def orchestrate():
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def _call():
            try:
                fut.set_result(run_coro_sync(produce()))
            except Exception as exc:  # pragma: no cover - debugging aid
                fut.set_exception(exc)

        loop.call_soon(_call)
        return await fut

    assert asyncio.run(orchestrate()) == 99


def test_sanitize_priority_urls_normalizes():
    urls = [
        " https://api.example.com/v1/?api_key=SECRET ",
        "https://API.example.com//v1/",  # dedupe via normalization
        "http://example.com/../v1",  # normalized path
        "mailto:foo@example.com",  # dropped
        "CHANGE_ME",  # placeholder
        "https://solscan.io/token/ABC?token=foo",  # query redaction
    ]
    cleaned = sanitize_priority_urls(urls)
    assert cleaned == [
        "https://api.example.com/v1?api_key=REDACTED",
        "https://api.example.com/v1",
        "http://example.com/v1",
        "https://solscan.io/token/ABC?token=REDACTED",
    ]


def test_sanitize_priority_urls_allow_non_http():
    urls = ["wss://example.com/stream"]
    assert sanitize_priority_urls(urls, allow_non_http=True) == ["wss://example.com/stream"]
