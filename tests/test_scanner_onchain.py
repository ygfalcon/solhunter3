import asyncio
import pytest
from solhunter_zero import scanner_onchain
from solders.pubkey import Pubkey as PublicKey


def setup_function(_):
    ttl = getattr(scanner_onchain, "METRIC_CACHE_TTL", scanner_onchain._METRIC_TTL)
    scanner_onchain.MEMPOOL_RATE_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=ttl
    )
    scanner_onchain.WHALE_ACTIVITY_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=ttl
    )
    scanner_onchain.AVG_SWAP_SIZE_CACHE = scanner_onchain.TTLCache(
        maxsize=256, ttl=ttl
    )

class FakeClient:
    def __init__(self, url, log=None):
        self.url = url
        self.log = log

    def get_program_accounts(
        self,
        program_id,
        *,
        encoding="jsonParsed",
        data_size=None,
        filters=None,
    ):
        if self.log is not None:
            self.log.append({"filters": filters, "data_size": data_size})
        assert encoding == "jsonParsed"
        assert isinstance(program_id, PublicKey)

        if filters == scanner_onchain._MINT_ACCOUNT_FILTERS:
            assert data_size == scanner_onchain.MINT_ACCOUNT_DATA_SIZE
            return {
                "result": [
                    {
                        "pubkey": "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1",
                        "account": {
                            "data": {"parsed": {"type": "mint", "info": {}}}
                        },
                    }
                ]
            }

        assert filters == scanner_onchain._TOKEN_ACCOUNT_FILTERS
        assert data_size == scanner_onchain.TOKEN_ACCOUNT_DATA_SIZE
        return {
            "result": [
                {
                    "pubkey": "Ignored",
                    "account": {
                        "data": {
                            "parsed": {
                                "type": "account",
                                "info": {"mint": "Mint2Mint2Mint2Mint2Mint2Mint2Mint2Mint2"},
                            }
                        }
                    },
                },
                {
                    "pubkey": "Ignored2",
                    "account": {
                        "data": {
                            "parsed": {
                                "type": "account",
                                "info": {"mint": "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1"},
                            }
                        }
                    },
                },
            ]
        }

def test_scan_tokens_onchain(monkeypatch):
    captured = {"calls": []}

    def fake_client(url):
        client = FakeClient(url, log=captured["calls"])
        captured['url'] = url
        return client

    monkeypatch.setattr(scanner_onchain, "Client", fake_client)
    tokens = scanner_onchain.scan_tokens_onchain_sync("http://node")
    assert captured['url'] == "http://node"
    assert tokens == [
        "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1",
        "Mint2Mint2Mint2Mint2Mint2Mint2Mint2Mint2",
    ]
    assert [entry["filters"] for entry in captured["calls"]] == [
        scanner_onchain._MINT_ACCOUNT_FILTERS,
        scanner_onchain._TOKEN_ACCOUNT_FILTERS,
    ]
    assert [entry["data_size"] for entry in captured["calls"]] == [
        scanner_onchain.MINT_ACCOUNT_DATA_SIZE,
        scanner_onchain.TOKEN_ACCOUNT_DATA_SIZE,
    ]


def test_scan_tokens_onchain_requires_url():
    with pytest.raises(ValueError):
        scanner_onchain.scan_tokens_onchain_sync("")


class FlakyClient:
    def __init__(self, url, state=None, log=None):
        self.url = url
        self.state = state if state is not None else {"total": 0, "mint": 0}
        self.log = log

    def get_program_accounts(
        self,
        program_id,
        *,
        encoding="jsonParsed",
        data_size=None,
        filters=None,
    ):
        assert encoding == "jsonParsed"
        assert isinstance(program_id, PublicKey)
        self.state["total"] = self.state.get("total", 0) + 1
        if self.log is not None:
            self.log.append({"filters": filters, "data_size": data_size})

        if filters == scanner_onchain._MINT_ACCOUNT_FILTERS:
            assert data_size == scanner_onchain.MINT_ACCOUNT_DATA_SIZE
            self.state["mint"] = self.state.get("mint", 0) + 1
            if self.state["mint"] < 3:
                raise Exception("rpc fail")
            return {
                "result": [
                    {
                        "pubkey": "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1",
                        "account": {
                            "data": {"parsed": {"type": "mint", "info": {}}}
                        },
                    }
                ]
            }

        assert filters == scanner_onchain._TOKEN_ACCOUNT_FILTERS
        assert data_size == scanner_onchain.TOKEN_ACCOUNT_DATA_SIZE
        return {
            "result": [
                {
                    "pubkey": "Ignored",
                    "account": {
                        "data": {
                            "parsed": {
                                "type": "account",
                                "info": {"mint": "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1"},
                            }
                        }
                    },
                }
            ]
        }


def test_scan_tokens_onchain_retries(monkeypatch):
    state = {"total": 0, "mint": 0}
    call_log = []

    def fake_client(url):
        return FlakyClient(url, state=state, log=call_log)

    sleeps = []

    monkeypatch.setattr(scanner_onchain, "Client", fake_client)
    async def _fake_sleep(t):
        sleeps.append(t)

    monkeypatch.setattr(scanner_onchain.asyncio, "sleep", _fake_sleep)

    tokens = scanner_onchain.scan_tokens_onchain_sync("http://node")

    assert tokens == ["Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1"]
    assert state["total"] == 4
    assert state["mint"] == 3
    assert sleeps == [1, 2]
    assert call_log[-1]["filters"] == scanner_onchain._TOKEN_ACCOUNT_FILTERS



def test_scan_tokens_onchain_helius_paginates(monkeypatch):
    requests_made = []

    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._payload

    class _Session:
        def __init__(self):
            self.calls = 0

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, *, json, timeout):
            requests_made.append({"url": url, "json": json, "timeout": timeout})
            filters = json["params"][1].get("filters")
            if filters == scanner_onchain._MINT_ACCOUNT_FILTERS:
                if self.calls == 0:
                    payload = {
                        "result": {
                            "accounts": [
                                {
                                    "pubkey": "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1",
                                    "account": {
                                        "data": {
                                            "parsed": {
                                                "type": "mint",
                                                "info": {}
                                            }
                                        }
                                    }
                                }
                            ],
                            "paginationKey": "next-page",
                        }
                    }
                else:
                    payload = {
                        "result": {
                            "accounts": [],
                        }
                    }
            else:
                payload = {
                    "result": {
                        "accounts": [
                            {
                                "account": {
                                    "data": {
                                        "parsed": {
                                            "type": "account",
                                            "info": {
                                                "mint": "Mint2Mint2Mint2Mint2Mint2Mint2Mint2Mint2"
                                            }
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            self.calls += 1
            return _Response(payload)

    monkeypatch.setattr(scanner_onchain.requests, "Session", lambda: _Session(), raising=False)

    tokens = scanner_onchain.scan_tokens_onchain_sync("https://rpc.helius.dev/?api-key=test")

    assert tokens == [
        "Mint1Mint1Mint1Mint1Mint1Mint1Mint1Mint1",
        "Mint2Mint2Mint2Mint2Mint2Mint2Mint2Mint2",
    ]
    assert len(requests_made) == 3
    first, second, third = requests_made
    assert first["json"]["method"] == "getProgramAccountsV2"
    assert first["json"]["params"][1]["limit"] == scanner_onchain._HELIUS_GPA_PAGE_LIMIT
    assert first["json"]["params"][1]["filters"] == scanner_onchain._MINT_ACCOUNT_FILTERS
    assert "paginationKey" not in first["json"]["params"][1]
    assert second["json"]["params"][1]["filters"] == scanner_onchain._MINT_ACCOUNT_FILTERS
    assert second["json"]["params"][1]["paginationKey"] == "next-page"
    assert third["json"]["params"][1]["filters"] == scanner_onchain._TOKEN_ACCOUNT_FILTERS
    assert "paginationKey" not in third["json"]["params"][1]


def test_mempool_tx_rate(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"blockTime": 1}, {"blockTime": 3}, {"blockTime": 4}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    rate = scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    assert rate == pytest.approx(3 / 3)


def test_whale_wallet_activity(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_token_largest_accounts(self, addr):
            return {"result": {"value": [{"uiAmount": 2000.0}, {"uiAmount": 50.0}]}}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    activity = scanner_onchain.fetch_whale_wallet_activity(
        "tok", "http://node", threshold=1000.0
    )
    assert activity == pytest.approx(2000.0 / 2050.0)


def test_average_swap_size(monkeypatch):
    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"amount": 2.0}, {"amount": 4.0}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    size = scanner_onchain.fetch_average_swap_size("tok", "http://node")
    assert size == pytest.approx(3.0)


def test_mempool_tx_rate_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            calls["count"] += 1
            return {"result": [{"blockTime": 1}, {"blockTime": 2}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)

    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")

    assert calls["count"] == 1


def test_whale_wallet_activity_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_token_largest_accounts(self, addr):
            calls["count"] += 1
            return {"result": {"value": [{"uiAmount": 2000.0}, {"uiAmount": 50.0}]}}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)

    scanner_onchain.fetch_whale_wallet_activity("tok", "http://node")
    scanner_onchain.fetch_whale_wallet_activity("tok", "http://node")

    assert calls["count"] == 1


def test_average_swap_size_cache(monkeypatch):
    calls = {"count": 0}

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            calls["count"] += 1
            return {"result": [{"amount": 2.0}, {"amount": 4.0}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)

    scanner_onchain.fetch_average_swap_size("tok", "http://node")
    scanner_onchain.fetch_average_swap_size("tok", "http://node")

    assert calls["count"] == 1


def test_mempool_tx_rate_model(monkeypatch, tmp_path):
    from solhunter_zero.models.onchain_forecaster import LSTMForecaster, save_model
    import pytest
    torch = pytest.importorskip("torch")

    model = LSTMForecaster(input_dim=4, hidden_dim=2, num_layers=1, seq_len=2)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.5)
    path = tmp_path / "oc.pt"
    save_model(model, path)
    monkeypatch.setenv("ONCHAIN_MODEL_PATH", str(path))

    class Client:
        def __init__(self, url):
            self.url = url

        def get_signatures_for_address(self, addr, limit=20):
            return {"result": [{"blockTime": 1}, {"blockTime": 2}]}

    monkeypatch.setattr(scanner_onchain, "Client", Client)
    monkeypatch.setattr(scanner_onchain, "PublicKey", lambda x: x)
    import types, sys
    oc = types.SimpleNamespace(order_book_depth_change=lambda *a, **k: 0.0)
    sys.modules["solhunter_zero.onchain_metrics"] = oc
    monkeypatch.setattr(scanner_onchain, "fetch_whale_wallet_activity", lambda t, u: 0.0)
    monkeypatch.setattr(scanner_onchain, "fetch_average_swap_size", lambda t, u: 0.0)

    scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    scanner_onchain.MEMPOOL_RATE_CACHE.clear()
    rate = scanner_onchain.fetch_mempool_tx_rate("tok", "http://node")
    assert rate == pytest.approx(0.5)

