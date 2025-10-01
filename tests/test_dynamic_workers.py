import asyncio
import importlib
import types
import sys


def _stub_torch():
    torch_mod = importlib.import_module("torch")
    torch_mod.cuda.is_available = lambda: True
    return torch_mod


def _stub_pl():
    pl_mod = types.ModuleType("pytorch_lightning")
    pl_mod.__spec__ = importlib.machinery.ModuleSpec("pytorch_lightning", None)
    pl_mod.callbacks = types.SimpleNamespace(Callback=object)
    pl_mod.LightningModule = object
    pl_mod.LightningDataModule = object
    pl_mod.Trainer = object
    return pl_mod


async def _noop(*_a, **_k):
    return None


def test_dynamic_workers_auto(monkeypatch):
    torch_mod = _stub_torch()
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setitem(sys.modules, "pytorch_lightning", _stub_pl())
    captured = {}
    class DummyDaemon:
        def __init__(self, *, dynamic_workers=False, **_):
            captured["dw"] = dynamic_workers
        def start(self, *a, **k):
            return asyncio.create_task(_noop())

    event_mod = types.ModuleType("solhunter_zero.event_bus")
    class _Sub:
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
    event_mod.subscription = lambda *_a, **_k: _Sub()
    event_mod.publish = lambda *_a, **_k: None
    event_mod.connect_broker = lambda *_a, **_k: None
    event_mod.subscribe = lambda *_a, **_k: lambda: None
    event_mod._BROKER_URLS = []
    monkeypatch.setitem(sys.modules, "solhunter_zero.event_bus", event_mod)
    rl_daemon_mod = types.ModuleType("solhunter_zero.rl_daemon")
    rl_daemon_mod.RLDaemon = DummyDaemon
    monkeypatch.setitem(sys.modules, "solhunter_zero.rl_daemon", rl_daemon_mod)
    import solhunter_zero.main as main_module
    monkeypatch.setattr(main_module.os, "cpu_count", lambda: 4)

    asyncio.run(main_module._init_rl_training({"rl_auto_train": True}, rl_daemon=True, rl_interval=0.01))
    assert captured["dw"] is True


def test_dynamic_workers_single_core(monkeypatch):
    torch_mod = _stub_torch()
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setitem(sys.modules, "pytorch_lightning", _stub_pl())
    captured = {}
    class DummyDaemon:
        def __init__(self, *, dynamic_workers=False, **_):
            captured["dw"] = dynamic_workers
        def start(self, *a, **k):
            return asyncio.create_task(_noop())

    event_mod = types.ModuleType("solhunter_zero.event_bus")
    class _Sub:
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            pass
    event_mod.subscription = lambda *_a, **_k: _Sub()
    event_mod.publish = lambda *_a, **_k: None
    event_mod.connect_broker = lambda *_a, **_k: None
    event_mod.subscribe = lambda *_a, **_k: lambda: None
    event_mod._BROKER_URLS = []
    monkeypatch.setitem(sys.modules, "solhunter_zero.event_bus", event_mod)
    rl_daemon_mod = types.ModuleType("solhunter_zero.rl_daemon")
    rl_daemon_mod.RLDaemon = DummyDaemon
    monkeypatch.setitem(sys.modules, "solhunter_zero.rl_daemon", rl_daemon_mod)
    import solhunter_zero.main as main_module
    monkeypatch.setattr(main_module.os, "cpu_count", lambda: 1)

    asyncio.run(main_module._init_rl_training({"rl_auto_train": True}, rl_daemon=True, rl_interval=0.01))
    assert captured["dw"] is False
