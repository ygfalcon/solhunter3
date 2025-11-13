import json
import logging
import sys
from logging.handlers import RotatingFileHandler

from solhunter_zero.logging_utils import (
    configure_runtime_logging,
    setup_stdout_logging,
)


def test_setup_stdout_logging_removes_duplicate_stream_handlers():
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)
        stderr_handler = logging.StreamHandler()
        stdout_alias = logging.StreamHandler(getattr(sys, "__stdout__", sys.stdout))
        root.addHandler(stderr_handler)
        root.addHandler(stdout_alias)

        handler = setup_stdout_logging()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) == 1
        assert stream_handlers[0] is handler
        assert handler.stream is sys.stdout
    finally:
        for handler in list(root.handlers):
            if handler not in original_handlers:
                root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)


def test_setup_stdout_logging_handles_alias_streams_by_fileno():
    class MirrorStream:
        def __init__(self, stream):
            self._stream = stream

        def write(self, data):  # pragma: no cover - exercised indirectly
            return self._stream.write(data)

        def flush(self):  # pragma: no cover - exercised indirectly
            return self._stream.flush()

        def fileno(self):  # pragma: no cover - simple delegation
            return self._stream.fileno()

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)
        alias_handler = logging.StreamHandler(MirrorStream(sys.stdout))
        direct_handler = logging.StreamHandler(sys.stdout)
        root.addHandler(alias_handler)
        root.addHandler(direct_handler)

        handler = setup_stdout_logging()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) == 1
        assert stream_handlers[0] is handler
        assert handler.stream is sys.stdout
    finally:
        for handler in list(root.handlers):
            if handler not in original_handlers:
                root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)


def test_configure_runtime_logging_deduplicates_file_handlers(tmp_path):
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

        log_path = tmp_path / "runtime.log"
        configure_runtime_logging(logfile=log_path, force=True)

        duplicate = RotatingFileHandler(log_path, encoding="utf-8")
        root.addHandler(duplicate)

        configure_runtime_logging(logfile=log_path, force=False)

        file_handlers = [
            h
            for h in root.handlers
            if getattr(h, "baseFilename", None) == str(log_path)
        ]
        assert len(file_handlers) == 1
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)


def test_json_formatter_redacts_credentials_in_message_and_extra() -> None:
    from solhunter_zero.logging_utils import JsonFormatter

    raw_url = "redis://user:secret@cache.example:6380/2?ssl=true"
    record = logging.LogRecord(
        "test", logging.INFO, __file__, 10, "redis url %s", (raw_url,), None
    )
    record.redis_url = raw_url

    formatter = JsonFormatter()
    payload = json.loads(formatter.format(record))

    redacted = "redis://****@cache.example:6380/2?ssl=true"
    assert payload["msg"].endswith(redacted)
    assert payload["redis_url"] == redacted
    assert "secret" not in json.dumps(payload)


def test_json_formatter_redacts_nested_structures() -> None:
    from solhunter_zero.logging_utils import JsonFormatter

    raw_url = "redis://user:password@cache.example:6379/1"
    record = logging.LogRecord("test", logging.INFO, __file__, 20, "ok", (), None)
    record.context = {
        "primary": raw_url,
        "replicas": [raw_url, "redis://cache.example:6379/1"],
    }

    formatter = JsonFormatter()
    payload = json.loads(formatter.format(record))

    assert payload["context"]["primary"] == "redis://****@cache.example:6379/1"
    assert payload["context"]["replicas"][0] == "redis://****@cache.example:6379/1"
    assert payload["context"]["replicas"][1] == "redis://cache.example:6379/1"
    assert "password" not in json.dumps(payload)
