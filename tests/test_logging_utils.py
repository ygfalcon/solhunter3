import logging
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
