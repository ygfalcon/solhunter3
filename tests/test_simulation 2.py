import numpy as np
import pytest
import asyncio
import time

from solhunter_zero import simulation, http
from solhunter_zero.simulation import SimulationResult, predict_price_movement


# reset global state before each test
def setup_function(_):
    http._session = None
    simulation.TOKEN_METRICS_CACHE = simulation.TTLCache(
        maxsize=256, ttl=simulation.TOKEN_METRICS_CACHE_TTL
    )
    simulation.invalidate_simulation_models()


def test_predict_price_movement_delegates(monkeypatch):
    captured = {}

    def fake_run(token, count=1, days=1, **_):
        captured["count"] = count
        captured["days"] = days
        return [SimulationResult(success_prob=1.0, expected_roi=0.12)]

    monkeypatch.setattr(simulation, "run_simulations", fake_run)

    val = predict_price_movement("tok")
    assert val == pytest.approx(0.12)
    assert captured["count"] == 1
    assert captured["days"] == 1


def test_run_simulations_uses_metrics(monkeypatch):
    def fake_metrics(token):
        return {
            "mean": 0.01,
            "volatility": 0.0,
            "volume": 50.0,
            "liquidity": 60.0,
            "slippage": 0.01,
            "depth": 1.0,
        }

    def fake_dex_metrics(token):
        return {"depth": 2.0}

    monkeypatch.setenv("SOLANA_RPC_URL", "http://node")
    monkeypatch.setattr(
        simulation.onchain_metrics,
        "fetch_volume_onchain",
        lambda t, u: 123.0,
    )
    monkeypatch.setattr(
        simulation.onchain_metrics,
        "fetch_liquidity_onchain",
        lambda t, u: 456.0,
    )
    monkeypatch.setattr(
        simulation.onchain_metrics,
        "fetch_slippage_onchain",
        lambda t, u: 0.01,
    )

    captured = {}

    def fake_normal(mean, vol, size):
        captured["mean"] = mean
        captured["vol"] = vol
        return np.full(size, mean)

    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    monkeypatch.setattr(
        simulation.onchain_metrics,
        "fetch_dex_metrics_async",
        fake_dex_metrics,
    )
    monkeypatch.setattr(simulation.np.random, "normal", fake_normal)

    results = simulation.run_simulations("tok", count=1, days=2)
    assert isinstance(results[0], SimulationResult)
    assert captured["mean"] == 0.01
    assert captured["vol"] == 0.0
    expected_roi = pytest.approx((1 + 0.01) ** 2 - 1)
    assert results[0].expected_roi == expected_roi
    assert results[0].volume == pytest.approx(123.0)
    assert results[0].liquidity == pytest.approx(456.0)
    assert results[0].slippage == pytest.approx(0.01)
    assert results[0].volume_spike == pytest.approx(1.0)


def test_fetch_token_metrics_base_url(monkeypatch):
    class FakeResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "mean_return": 0.1,
                "volatility": 0.03,
                "volume_24h": 321.0,
                "liquidity": 654.0,
                "slippage": 0.02,
            }

    captured = {}

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=5):
            captured["url"] = url
            return FakeResp()

    monkeypatch.setenv("METRICS_BASE_URL", "http://metrics.local")
    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())

    metrics = simulation.fetch_token_metrics("tok")
    assert captured["url"] == "http://metrics.local/token/tok/metrics"
    assert metrics["mean"] == pytest.approx(0.1)
    assert metrics["volatility"] == pytest.approx(0.03)
    assert metrics["volume"] == pytest.approx(321.0)
    assert metrics["liquidity"] == pytest.approx(654.0)
    assert metrics["slippage"] == pytest.approx(0.02)


def test_fetch_token_metrics_multiple_dex(monkeypatch):
    urls = []

    class FakeResp:
        def __init__(self, url):
            self.url = url

        def raise_for_status(self):
            pass

        def json(self):
            if "metrics" in self.url:
                return {"mean_return": 0.0, "volatility": 0.02}
            if "dex1" in self.url and "depth" in self.url:
                return {"depth": 1.0}
            if "dex1" in self.url and "slippage" in self.url:
                return {"slippage": 0.01}
            if "dex2" in self.url and "depth" in self.url:
                return {"depth": 2.0}
            return {"slippage": 0.02}

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=5):
            urls.append(url)
            return FakeResp(url)

    monkeypatch.setenv("DEX_METRIC_URLS", "http://dex1,http://dex2")
    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())

    metrics = simulation.fetch_token_metrics("tok")

    assert "http://dex1/v1/depth?token=tok" in urls
    assert "http://dex2/v1/slippage?token=tok" in urls
    assert metrics["depth"] == pytest.approx(1.5)
    assert metrics["slippage"] == pytest.approx(0.015)
    assert metrics["depth_per_dex"] == [1.0, 2.0]
    assert metrics["slippage_per_dex"] == [0.01, 0.02]


def test_fetch_token_metrics_concurrent(monkeypatch):
    calls = []

    class FakeResp:
        def __init__(self, url):
            self.url = url

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            await asyncio.sleep(0.05)
            if "depth" in self.url:
                return {"depth": 1.0}
            if "slippage" in self.url:
                return {"slippage": 0.1}
            return {"mean_return": 0.0, "volatility": 0.02}

    class FakeSession:
        def get(self, url, timeout=5):
            calls.append(url)
            return FakeResp(url)

    monkeypatch.setenv("DEX_METRIC_URLS", "http://d1,http://d2")
    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())

    start = time.perf_counter()
    metrics = simulation.fetch_token_metrics("tok")
    elapsed = time.perf_counter() - start

    assert metrics["depth"] == pytest.approx(1.0)
    assert metrics["slippage"] == pytest.approx(0.1)
    assert elapsed < 0.12


def test_token_metrics_cache(monkeypatch):
    calls = {"sessions": 0, "gets": 0}

    class FakeResp:
        def __init__(self, url):
            calls["url"] = url

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return {"mean_return": 0.0, "volatility": 0.02}

    class FakeSession:
        def __init__(self):
            calls["sessions"] += 1
            self.closed = False

        def get(self, url, timeout=5):
            calls["gets"] += 1
            return FakeResp(url)

    monkeypatch.setenv("DEX_METRIC_URLS", "")
    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())
    simulation.TOKEN_METRICS_CACHE.ttl = 60

    metrics1 = simulation.fetch_token_metrics("tok")
    metrics2 = simulation.fetch_token_metrics("tok")

    assert metrics1 == metrics2
    assert calls["sessions"] == 1
    assert calls["gets"] == 1


def test_run_simulations_volume_filter(monkeypatch):
    def fake_metrics(token):
        return {
            "mean": 0.01,
            "volatility": 0.02,
            "volume": 50.0,
            "liquidity": 100.0,
            "slippage": 0.05,
        }

    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )

    results = simulation.run_simulations("tok", count=1, min_volume=100.0)
    assert results == []


def test_run_simulations_recent_volume(monkeypatch):
    def fake_metrics(token):
        return {
            "mean": 0.01,
            "volatility": 0.02,
            "volume": 50.0,
            "liquidity": 100.0,
            "slippage": 0.05,
        }

    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )

    results = simulation.run_simulations("tok", count=1, recent_volume=150.0)
    assert results[0].volume == pytest.approx(150.0)
    assert results[0].volume_spike == pytest.approx(3.0)


def test_run_simulations_with_history(monkeypatch):
    metrics = {
        "mean": 0.01,
        "volatility": 0.02,
        "volume": 50.0,
        "liquidity": 100.0,
        "slippage": 0.05,
        "depth": 1.0,
        "price_history": [1.0, 1.1, 1.05],
        "liquidity_history": [90, 95, 100],
        "depth_history": [0.8, 0.9, 1.0],
        "slippage_history": [0.04, 0.045, 0.05],
        "depth_per_dex": [0.5, 0.6],
        "slippage_per_dex": [0.02, 0.03],
    }

    captured = {}

    class FakeGBR:
        def fit(self, X, y):
            captured["X"] = X
            captured["y"] = y
            return self

        def predict(self, X):
            captured["predict_X"] = X
            return np.array([0.07])

    monkeypatch.setattr(simulation, "fetch_token_metrics", lambda _t: metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )
    monkeypatch.setattr(simulation, "GradientBoostingRegressor", lambda: FakeGBR())
    monkeypatch.setattr(
        simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean)
    )

    results = simulation.run_simulations("tok", count=1, days=2)

    assert len(captured["predict_X"][0]) == 7
    expected_roi = pytest.approx((1 + 0.07) ** 2 - 1)
    assert results[0].expected_roi == expected_roi


def test_run_simulations_with_tx_trend(monkeypatch):
    metrics = {
        "mean": 0.01,
        "volatility": 0.02,
        "volume": 50.0,
        "liquidity": 100.0,
        "slippage": 0.05,
        "depth": 1.0,
        "price_history": [1.0, 1.1, 1.05],
        "liquidity_history": [40, 45, 50],
        "depth_history": [0.7, 0.8, 1.0],
        "slippage_history": [0.04, 0.045, 0.05],
        "tx_count_history": [10, 15, 20, 30],
        "depth_per_dex": [0.5, 0.6],
        "slippage_per_dex": [0.02, 0.03],
        "token_age": 5.0,
        "initial_liquidity": 40.0,
    }

    captured = {}

    class FakeRF:
        def fit(self, X, y):
            captured["X"] = X
            captured["y"] = y
            return self

        def predict(self, X):
            captured["predict_X"] = X
            return np.array([0.09])

    monkeypatch.setattr(simulation, "fetch_token_metrics", lambda _t: metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )
    monkeypatch.setattr(simulation, "RandomForestRegressor", lambda **kw: FakeRF())
    monkeypatch.setattr(simulation, "XGBRegressor", None)
    monkeypatch.setattr(simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean))

    results = simulation.run_simulations("tok", count=1, days=2)

    assert len(captured["predict_X"][0]) == 10
    expected_roi = pytest.approx((1 + 0.09) ** 2 - 1)
    assert results[0].expected_roi == expected_roi


def test_run_simulations_optional_inputs(monkeypatch):
    def fake_metrics(token):
        return {
            "mean": 0.0,
            "volatility": 0.02,
            "volume": 10.0,
            "liquidity": 20.0,
            "slippage": 0.01,
        }

    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )
    monkeypatch.setattr(simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean))

    res = simulation.run_simulations(
        "tok",
        count=1,
        sentiment=0.8,
        order_book_strength=0.9,
    )[0]

    assert res.sentiment == pytest.approx(0.8)
    assert res.order_book_strength == pytest.approx(0.9)



def test_run_simulations_additional_metrics(monkeypatch):
    def fake_metrics(token):
        return {
            "mean": 0.0,
            "volatility": 0.02,

            "volume": 10.0,
            "liquidity": 20.0,
            "slippage": 0.01,
        }


    monkeypatch.setenv("SOLANA_RPC_URL", "http://node")
    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )
    monkeypatch.setattr(
        simulation.onchain_metrics,
        "collect_onchain_insights",
        lambda t, u: {"depth_change": 1.0, "tx_rate": 2.0, "whale_activity": 0.5},
    )
    monkeypatch.setattr(simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean))

    res = simulation.run_simulations("tok", count=1)[0]
    assert res.depth_change == pytest.approx(1.0)
    assert res.tx_rate == pytest.approx(2.0)
    assert res.whale_activity == pytest.approx(0.5)

