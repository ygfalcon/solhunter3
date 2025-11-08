import pytest

from solhunter_zero.agents.discovery import DiscoveryAgent


@pytest.mark.parametrize(
    "value, expected",
    [
        ("api.mainnet-beta.solana.com", "ws://api.mainnet-beta.solana.com"),
        (
            "http://api.mainnet-beta.solana.com",
            "ws://api.mainnet-beta.solana.com",
        ),
        (
            "https://api.mainnet-beta.solana.com",
            "wss://api.mainnet-beta.solana.com",
        ),
        ("ws://existing.endpoint", "ws://existing.endpoint"),
    ],
)
def test_as_websocket_url_variants(value: str, expected: str) -> None:
    assert DiscoveryAgent._as_websocket_url(value) == expected
