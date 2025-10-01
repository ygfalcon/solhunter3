import importlib
import sys
import types
from pathlib import Path


def test_configure_startup_env_only_once(monkeypatch):
    dummy_pydantic = types.SimpleNamespace(
        BaseModel=object,
        AnyUrl=str,
        ValidationError=Exception,
        root_validator=lambda *a, **k: (lambda f: f),
        validator=lambda *a, **k: (lambda f: f),
        field_validator=lambda *a, **k: (lambda f: f),
        model_validator=lambda *a, **k: (lambda f: f),
    )
    monkeypatch.setitem(sys.modules, "pydantic", dummy_pydantic)

    import solhunter_zero.env_config as env_config

    mod = importlib.reload(env_config)
    calls = []

    def fake_config(root=None):
        calls.append(root)
        return {"DUMMY": "1"}

    monkeypatch.setattr(mod, "configure_environment", fake_config)
    mod.configure_startup_env(Path("a"))
    mod.configure_startup_env(Path("b"))
    assert calls == [Path("a")]
