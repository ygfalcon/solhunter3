import socket

import pytest

from solhunter_zero.ui import UIState, UIServer


def test_ui_server_start_port_conflict_raises() -> None:
    state = UIState()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        host, port = sock.getsockname()

        server = UIServer(state, host=host, port=port)

        with pytest.raises(OSError):
            server.start()

        assert server._server is None
        assert not (server._thread and server._thread.is_alive())


def test_ui_server_start_success_sets_server_and_thread() -> None:
    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server._server is not None
        assert server._thread is not None
        assert server._thread.is_alive()
    finally:
        server.stop()


def test_ui_server_start_blocks_until_port_reachable() -> None:
    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        with socket.create_connection((server.host, server.port), timeout=1):
            pass
    finally:
        server.stop()
