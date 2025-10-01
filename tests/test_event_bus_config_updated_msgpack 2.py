import importlib
import json
import types


def test_config_updated_event_msgpack(monkeypatch):
    monkeypatch.setenv("EVENT_SERIALIZATION", "msgpack")
    import solhunter_zero.event_bus as eb

    importlib.reload(eb)

    cfg = {"foo": "bar"}

    encoded = eb._encode_event("config_updated", cfg)
    data = json.loads(encoded.decode())
    assert json.loads(data["config_updated"]["config_json"]) == cfg

    dummy = types.SimpleNamespace(config_json=json.dumps(cfg))
    ev = types.SimpleNamespace(
        config_updated=dummy, WhichOneof=lambda _: "config_updated"
    )
    assert eb._decode_payload(ev) == cfg

    received: list[dict] = []
    with eb.subscription("config_updated", received.append):
        eb.publish("config_updated", cfg)

    assert received == [cfg]

    monkeypatch.delenv("EVENT_SERIALIZATION", raising=False)
    importlib.reload(eb)

