import logging
import os
import sys

from solhunter_zero.logging_utils import setup_stdout_logging


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


def test_setup_stdout_logging_handles_fd_aliases():
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    duplicate_stream = None
    try:
        for handler in list(root.handlers):
            root.removeHandler(handler)

        handler_stdout = logging.StreamHandler(sys.stdout)
        root.addHandler(handler_stdout)

        duplicate_fd = os.dup(sys.stdout.fileno())
        duplicate_stream = os.fdopen(duplicate_fd, "w", closefd=True)
        handler_duplicate = logging.StreamHandler(duplicate_stream)
        root.addHandler(handler_duplicate)

        handler = setup_stdout_logging()

        stream_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) == 1
        assert stream_handlers[0] is handler
        assert handler.stream is sys.stdout
    finally:
        if duplicate_stream is not None:
            try:
                duplicate_stream.close()
            except Exception:
                pass
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
