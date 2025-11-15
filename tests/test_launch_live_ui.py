import errno
import os

import pytest

import solhunter_zero.ui as ui


def test_ui_server_tears_down_on_bind_failure(monkeypatch):
    # Ensure we start from a clean environment
    ui._teardown_ui_environment()
    monkeypatch.setattr(ui, "_ENV_BOOTSTRAPPED", False)
    monkeypatch.setattr(ui, "load_production_env", lambda: None)
    for name in ("SOLHUNTER_MODE", "BROKER_CHANNEL", "REDIS_URL"):
        monkeypatch.delenv(name, raising=False)

    failure = OSError(errno.EADDRINUSE, "Address already in use")
    monkeypatch.setattr(ui, "make_server", lambda *args, **kwargs: (_ for _ in ()).throw(failure))

    server = ui.UIServer(ui.UIState())

    # Environment defaults should be in place before start attempts
    assert os.environ["SOLHUNTER_MODE"] == "live"
    assert ui._ENV_BOOTSTRAPPED is True
    assert ui._get_active_ui_state() is server.state

    with pytest.raises(RuntimeError):
        server.start()

    assert ui._ENV_BOOTSTRAPPED is False
    assert ui._get_active_ui_state() is None
