import sys
import types

import pytest


def _install_torch_stub() -> None:
    torch_mod = types.ModuleType("torch")
    torch_mod.Tensor = type("_Tensor", (), {})
    torch_mod.device = type("_Device", (), {"__init__": lambda self, *a, **k: None})
    nn_module = types.ModuleType("torch.nn")
    nn_module.Module = object
    nn_module.ModuleList = list
    nn_module.Sequential = lambda *args, **kwargs: list(args)
    functional_module = types.ModuleType("torch.nn.functional")
    optim_module = types.ModuleType("torch.optim")
    optim_module.Adam = lambda *args, **kwargs: None
    utils_module = types.ModuleType("torch.utils")
    data_module = types.ModuleType("torch.utils.data")
    data_module.Dataset = object
    data_module.DataLoader = list
    torch_mod.nn = nn_module
    torch_mod.optim = optim_module
    torch_mod.utils = utils_module
    nn_module.functional = functional_module
    utils_module.data = data_module
    torch_mod.__path__ = []  # type: ignore[attr-defined]
    nn_module.__path__ = []  # type: ignore[attr-defined]
    utils_module.__path__ = []  # type: ignore[attr-defined]
    data_module.__path__ = []  # type: ignore[attr-defined]
    sys.modules.setdefault("torch", torch_mod)
    sys.modules.setdefault("torch.nn", nn_module)
    sys.modules.setdefault("torch.nn.functional", functional_module)
    sys.modules.setdefault("torch.optim", optim_module)
    sys.modules.setdefault("torch.utils", utils_module)
    sys.modules.setdefault("torch.utils.data", data_module)

    datasets_pkg = types.ModuleType("solhunter_zero.datasets")
    datasets_pkg.__path__ = []  # type: ignore[attr-defined]
    sample_ticks_module = types.ModuleType("solhunter_zero.datasets.sample_ticks")
    sample_ticks_module.load_sample_ticks = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    sample_ticks_module.DEFAULT_PATH = ""  # type: ignore[attr-defined]
    datasets_pkg.sample_ticks = sample_ticks_module  # type: ignore[attr-defined]
    sys.modules.setdefault("solhunter_zero.datasets", datasets_pkg)
    sys.modules.setdefault("solhunter_zero.datasets.sample_ticks", sample_ticks_module)


_install_torch_stub()

from solhunter_zero.swarm_pipeline import _extract_realized_metrics


def test_extract_realized_metrics_sell_roi_from_fills():
    action = {"side": "sell", "entry_price": 1.0}
    result = {"fills": [{"price": 1.2, "filled_amount": 2.0}]}

    metrics = _extract_realized_metrics(action, result)

    assert metrics["realized_amount"] == pytest.approx(2.0)
    assert metrics["realized_price"] == pytest.approx(1.2)
    assert metrics["realized_roi"] == pytest.approx(0.2)


def test_extract_realized_metrics_buy_roi_from_fills():
    action = {"side": "buy", "entry_price": 1.5}
    result = {"fills": [{"price": 1.2, "filled_amount": 1.0}]}

    metrics = _extract_realized_metrics(action, result)

    assert metrics["realized_amount"] == pytest.approx(1.0)
    assert metrics["realized_price"] == pytest.approx(1.2)
    assert metrics["realized_roi"] == pytest.approx((1.5 - 1.2) / 1.5)


@pytest.mark.parametrize(
    "action,result",
    [
        ({"side": "sell"}, {"fills": [{"price": 1.2, "filled_amount": 1.0}]}),
        (
            {"side": "buy", "entry_price": 1.5},
            {"fills": [{"price": 1.2, "filled_amount": 0.0}]},
        ),
        ({"side": "hold", "entry_price": 1.5}, {"fills": []}),
    ],
)
def test_extract_realized_metrics_roi_defaults_to_none(action, result):
    metrics = _extract_realized_metrics(action, result)

    assert "realized_roi" in metrics
    assert metrics["realized_roi"] is None
