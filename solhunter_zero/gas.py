import os
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient

RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d")
RPC_TESTNET_URL = os.getenv(
    "SOLANA_TESTNET_RPC_URL",
    "https://devnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
)

LAMPORTS_PER_SOL = 1_000_000_000


def _extract_lamports(resp: object) -> int:
    """Return lamports per signature from an RPC response."""
    try:
        value = resp["value"]  # type: ignore[index]
    except Exception:
        value = getattr(resp, "value", {})
    if isinstance(value, dict):
        calc = value.get("feeCalculator") or value.get("fee_calculator") or {}
        lamports = calc.get("lamportsPerSignature") or calc.get(
            "lamports_per_signature"
        )
        if isinstance(lamports, (int, float)):
            return int(lamports)
    try:
        return int(value.fee_calculator.lamports_per_signature)  # type: ignore[attr-defined]
    except Exception:
        return 0


_FALLBACK_LAMPORTS = 5000  # network base fee per signature


def get_current_fee(testnet: bool = False) -> float:
    """Return current fee per signature in SOL."""
    client = Client(RPC_TESTNET_URL if testnet else RPC_URL)
    try:
        resp = client.get_latest_blockhash()
    except Exception:
        lamports = _FALLBACK_LAMPORTS
    else:
        lamports = _extract_lamports(resp) or _FALLBACK_LAMPORTS
    return lamports / LAMPORTS_PER_SOL


async def get_current_fee_async(testnet: bool = False) -> float:
    """Asynchronously return current fee per signature in SOL."""
    async with AsyncClient(RPC_TESTNET_URL if testnet else RPC_URL) as client:
        try:
            resp = await client.get_latest_blockhash()
        except Exception:
            lamports = _FALLBACK_LAMPORTS
        else:
            lamports = _extract_lamports(resp) or _FALLBACK_LAMPORTS
    return lamports / LAMPORTS_PER_SOL


async def get_priority_fee_async(rpc_url: str) -> float:
    """Return prioritization fee in SOL from ``rpc_url``."""

    async with AsyncClient(rpc_url) as client:
        try:
            resp = await client._provider.make_request(
                "getRecentPrioritizationFees",
                [],
            )
            lamports = int(resp["result"][0].get("prioritizationFee", 0))
        except Exception:
            lamports = 0
    return lamports / LAMPORTS_PER_SOL


async def get_priority_fee_estimate(rpc_urls: list[str]) -> float:
    """Return first non-zero prioritization fee from ``rpc_urls``."""

    for url in rpc_urls:
        fee = await get_priority_fee_async(url)
        if fee > 0:
            return fee
    return 0.0


def adjust_priority_fee(tx_rate: float) -> int:
    """Return compute-unit price in lamports based on ``tx_rate``."""

    if tx_rate <= 0:
        return 0

    # Linear scaling - 1000 lamports per tx/s with an upper cap
    price = int(tx_rate * 1000)
    return min(price, 10_000)
