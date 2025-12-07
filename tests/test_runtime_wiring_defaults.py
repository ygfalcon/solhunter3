from __future__ import annotations

import importlib
import importlib.util
import pathlib
import sys
import types

import solhunter_zero


def load_runtime_wiring_module():
    module_path = pathlib.Path(__file__).resolve().parents[1] / "solhunter_zero" / "runtime" / "runtime_wiring.py"

    runtime_pkg = sys.modules.get("solhunter_zero.runtime")
    if runtime_pkg is None:
        runtime_pkg = types.ModuleType("solhunter_zero.runtime")
        runtime_pkg.__path__ = [str(module_path.parent)]
        sys.modules["solhunter_zero.runtime"] = runtime_pkg

    # Provide lightweight stubs for dependencies pulled in at import time.
    event_bus_stub = types.ModuleType("solhunter_zero.event_bus")
    event_bus_stub.subscribe = lambda *args, **kwargs: None
    event_bus_stub.publish = lambda *args, **kwargs: None
    sys.modules.setdefault("solhunter_zero.event_bus", event_bus_stub)
    sys.modules.setdefault("solhunter_zero.runtime.event_bus", event_bus_stub)

    ui_stub = types.SimpleNamespace(UIState=type("UIState", (), {}), get_ws_channel_metrics=lambda *_, **__: {})
    ui_module = types.ModuleType("solhunter_zero.ui")
    ui_module.UIState = ui_stub.UIState
    ui_module.get_ws_channel_metrics = ui_stub.get_ws_channel_metrics
    sys.modules.setdefault("solhunter_zero.ui", ui_module)
    sys.modules.setdefault("solhunter_zero.runtime.ui", ui_module)

    util_module = types.ModuleType("solhunter_zero.util")
    util_module.parse_bool_env = lambda *_args, **_kwargs: False
    sys.modules.setdefault("solhunter_zero.util", util_module)
    sys.modules.setdefault("solhunter_zero.runtime.util", util_module)

    schema_adapters_module = types.ModuleType("solhunter_zero.runtime.schema_adapters")
    schema_adapters_module.read_golden = lambda *_args, **_kwargs: None
    schema_adapters_module.read_ohlcv = lambda *_args, **_kwargs: None
    sys.modules.setdefault("solhunter_zero.runtime.schema_adapters", schema_adapters_module)

    spec = importlib.util.spec_from_file_location(
        "solhunter_zero.runtime.runtime_wiring",
        module_path,
        submodule_search_locations=runtime_pkg.__path__,
    )
    assert spec and spec.loader, "Expected runtime_wiring module spec to be loadable"

    module = importlib.util.module_from_spec(spec)
    sys.modules["solhunter_zero.runtime.runtime_wiring"] = module
    spec.loader.exec_module(module)
    return module


def test_default_runtime_workflow_matches_launch_script_expectation():
    runtime_wiring = load_runtime_wiring_module()
    assert runtime_wiring.DEFAULT_RUNTIME_WORKFLOW == "golden-multi-stage-golden-stream"
