import json
import os
import urllib.parse
import urllib.request

import pytest

from solhunter_zero import prices


@pytest.mark.parametrize(
    "price_id",
    [value for _, value in sorted(prices.DEFAULT_PYTH_PRICE_IDS.items())],
)
def test_normalize_default_pyth_ids_length(price_id: str) -> None:
    normalized = prices._normalize_pyth_id(price_id)
    assert normalized.startswith("0x")
    assert len(normalized) == 66


def test_build_pyth_boot_url_uses_repeated_ids() -> None:
    base_url = "https://hermes.pyth.network/v2/price_feeds?chain=solana-mainnet"
    feed_ids = [
        "0x" + "ab" * 32,
        "0x" + "cd" * 32,
    ]
    accounts = [
        "J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS",
    ]
    url = prices._build_pyth_boot_url(
        base_url, feed_ids=feed_ids, accounts=accounts
    )
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert parsed.path == "/v2/price_feeds"
    assert query["chain"] == ["solana-mainnet"]
    assert query["ids[]"] == feed_ids
    assert query["accounts[]"] == accounts
    assert "id" not in query


@pytest.mark.skipif(
    not os.getenv("PYTH_HTTP_TEST"),
    reason="requires PYTH_HTTP_TEST=1 and outbound network access",
)
def test_pyth_endpoint_returns_payload() -> None:
    default_feed = next(iter(prices.DEFAULT_PYTH_PRICE_IDS.values()))
    normalized = prices._normalize_pyth_id(default_feed)
    url = prices._build_pyth_boot_url(
        prices.PYTH_PRICE_URL, feed_ids=[normalized], accounts=[]
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200
        payload = json.load(resp)
    assert isinstance(payload, list)
    assert payload, "expected non-empty response from Pyth"
    first = payload[0]
    assert isinstance(first, dict)
    assert "id" in first
