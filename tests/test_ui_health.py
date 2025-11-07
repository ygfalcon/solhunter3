import pytest

from solhunter_zero.ui import UIState, create_app


class FakeState(UIState):
    def __init__(self, snapshot):
        super().__init__()
        self._snapshot = snapshot

    def snapshot_status(self):  # type: ignore[override]
        return dict(self._snapshot)


@pytest.fixture
def client():
    def _make(snapshot):
        state = FakeState(snapshot)
        app = create_app(state)
        return app.test_client()

    return _make


def test_health_endpoint_reports_healthy(client):
    snapshot = {"event_bus": True, "trading_loop": 1, "heartbeat": 123.45}
    response = client(snapshot).get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "ok": True,
        "components": {
            "event_bus": True,
            "trading_loop": True,
            "heartbeat": True,
        },
        "status": snapshot,
    }


def test_health_endpoint_healthy_before_first_heartbeat(client):
    snapshot = {"event_bus": True, "trading_loop": True, "heartbeat": None}
    response = client(snapshot).get("/health")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload == {
        "ok": True,
        "components": {
            "event_bus": True,
            "trading_loop": True,
            "heartbeat": True,
        },
        "status": snapshot,
    }


def test_health_endpoint_reports_failures(client):
    snapshot = {"event_bus": False, "trading_loop": None, "heartbeat": "offline"}
    response = client(snapshot).get("/health")
    assert response.status_code == 503
    payload = response.get_json()
    assert payload["ok"] is False
    assert payload["failing"] == ["event_bus", "heartbeat", "trading_loop"]
    assert payload["components"] == {
        "event_bus": False,
        "trading_loop": False,
        "heartbeat": False,
    }
    assert payload["status"] == snapshot
