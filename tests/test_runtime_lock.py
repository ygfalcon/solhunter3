import importlib
import logging
from pathlib import Path

import pytest


def test_acquire_runtime_lock_permission_denied(monkeypatch, tmp_path, caplog):
    start_all = importlib.import_module("scripts.start_all")

    lock_path = tmp_path / "runtime.lock"
    lock_path.write_text("123\n", encoding="utf-8")

    monkeypatch.setattr(start_all, "_process_alive", lambda pid: False)

    original_unlink = Path.unlink

    def fake_unlink(self, *args, **kwargs):
        if self == lock_path:
            raise PermissionError("mocked permission error")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fake_unlink)

    with caplog.at_level(logging.WARNING):
        with pytest.raises(SystemExit) as excinfo:
            with start_all._acquire_runtime_lock(lock_path):
                pass

    assert "insufficient permissions" in caplog.text
    assert "123" in str(excinfo.value)
