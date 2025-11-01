import logging
import os

import pytest

from solhunter_zero import main


def test_config_load_failure_not_reported_as_depth_service(monkeypatch, caplog):
    error_message = "config load exploded"

    def fail(*_a, **_k):
        raise ValueError(error_message)

    monkeypatch.setattr(main, "perform_startup", fail)

    original_depth_env = os.environ.pop("DEPTH_SERVICE", None)
    caplog.set_level(logging.ERROR)

    try:
        with pytest.raises(ValueError):
            main.main(dry_run=True, offline=True)
    finally:
        if original_depth_env is not None:
            os.environ["DEPTH_SERVICE"] = original_depth_env
        else:
            os.environ.pop("DEPTH_SERVICE", None)

    assert "Failed to start depth_service" not in caplog.text
    assert error_message in caplog.text
    assert os.environ.get("DEPTH_SERVICE") != "false"
