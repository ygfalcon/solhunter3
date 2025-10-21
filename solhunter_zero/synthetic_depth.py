"""Synthetic market depth reconstruction utilities.

This module combines live order book data pulled directly from on-chain
sources with AMM reserve estimates and reference oracle pricing to produce a
stable depth estimate per mint.  The implementation deliberately avoids
external paid indexers; every data source is either on-chain or keyless.
"""

from __future__ import annotations

import asyncio
import base64
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import aiohttp
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

from .http import get_session
from .prices import _parse_pyth_mapping, fetch_price_quotes_async
from .token_aliases import canonical_mint

__all__ = [
    "compute_depth_change",
    "get_cached_snapshot",
]

# ---------------------------------------------------------------------------
# Constants and configuration
# ---------------------------------------------------------------------------

_JUP_MARKETS_URL = os.getenv("JUP_MARKETS_URL", "https://cache.jup.ag/markets")
_JUP_TOKENS_URL = os.getenv("JUP_TOKENS_URL", "https://cache.jup.ag/tokens")
_DEXSCREENER_URL = os.getenv(
    "DEXSCREENER_API_URL", "https://api.dexscreener.com/latest/dex/tokens"
)
_PYTH_HERMES_PRICE_URL = os.getenv(
    "PYTH_HERMES_PRICE_URL", "https://hermes.pyth.network/api/latest_price_feeds"
)

# Serum/OpenBook orderbook accounts encode a 13 byte prefix before the slab
# header (5 byte padding + 8 byte AccountFlags).  Skip this many bytes when
# decoding account data into the slab representation.
_ORDERBOOK_HEADER_PREFIX = 13

# Cache TTLs (seconds)
_MARKET_CACHE_TTL = float(os.getenv("SYNTH_DEPTH_MARKET_TTL", "300") or 300)
_TOKEN_CACHE_TTL = float(os.getenv("SYNTH_DEPTH_TOKEN_TTL", "900") or 900)
_DEX_CACHE_TTL = float(os.getenv("SYNTH_DEPTH_DEX_TTL", "45") or 45)
_PYTH_CACHE_TTL = float(os.getenv("SYNTH_DEPTH_PYTH_TTL", "2.0") or 2.0)

# Maximum number of L2 levels retained per side when aggregating order books.
_MAX_LEVELS = int(os.getenv("SYNTH_DEPTH_MAX_LEVELS", "20") or 20)

# Stablecoins default to $1 when oracle coverage is missing.
_STABLE_MINTS = {
    # USDC, USDT, USDCet, USDTet, UXD, Dai, USDH
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "Es9vMFrzaCERzSi1jS6t4G8iKrrf5gkP8KkP4dDLf2N9",
    "7XS2hK5gj4rvVhoKjP88vCcYqP3swa9uRc42sQVAy1Pa",
    "G4dndwpFNKp8a7LwwvHpTsB8FoD7RZxAvh9u7Pajztv5",
    "7kbnvuGBxxj8AG9qp8Scn56muWGaRaFqxg1FsRp3PaFT",
    "6ZgDSecY3kJ8ukG2qajcnhksor2m1YjDCE9R39KibqN9",
    "USDHd9scA3Qh8BTZpS4xVxXTJ7Q61zS3USDDf9P8it7",
}

# Liquidity class calibration (threshold_usd, depth_fraction, alpha, tau, spread_floor)
_LIQUIDITY_CLASSES: List[Tuple[float, float, float, float, float, str]] = [
    (5_000_000.0, 0.22, 0.75, 240.0, 12.0, "mega"),
    (1_000_000.0, 0.18, 0.65, 180.0, 18.0, "large"),
    (250_000.0, 0.12, 0.55, 150.0, 26.0, "mid"),
    (50_000.0, 0.08, 0.45, 120.0, 40.0, "small"),
    (0.0, 0.05, 0.35, 90.0, 65.0, "thin"),
]

_DEFAULT_RPC_URL = os.getenv(
    "SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"
)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Level:
    """Aggregated order book level in USD terms."""

    price: float
    size_base: float
    notional_usd: float


@dataclass(slots=True)
class OrderbookData:
    """Lightweight representation of combined order book state."""

    bids: List[Level]
    asks: List[Level]
    total_bid_usd: float
    total_ask_usd: float
    best_bid: Optional[float]
    best_ask: Optional[float]

    def spread_bps(self, mid: float) -> Optional[float]:
        if mid <= 0 or self.best_bid is None or self.best_ask is None:
            return None
        if self.best_ask <= 0:
            return None
        spread = self.best_ask - self.best_bid
        return max(0.0, (spread / mid) * 10_000)


@dataclass(slots=True)
class DexStats:
    """DexScreener liquidity summary for a mint."""

    liquidity_usd: float
    volume_24h: float
    best_price: float
    venues: List[str] = field(default_factory=list)
    best_pair: Optional[str] = None


@dataclass(slots=True)
class PythPrice:
    """Hermes reference price payload."""

    price: float
    confidence: float
    publish_time: float
    source: str = "pyth"


@dataclass(slots=True)
class SynthInputs:
    """Bundle of inputs used to compute synthetic depth."""

    pyth: Optional[PythPrice]
    orderbook: Optional[OrderbookData]
    dex: Optional[DexStats]
    mid_price_hint: float


@dataclass(slots=True)
class SyntheticState:
    """Tracked synthetic depth snapshot for a mint."""

    depth_usd: float
    last_update: float
    mid_usd: float
    spread_bps: float
    liquidity_class: str
    confidence_bps: float
    publish_time: float
    depth_pct: Dict[str, float]
    alpha: float
    tau: float
    extras: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Slab decoding helpers (minimal port of the Serum slab layout)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _SlabHeader:
    bump_index: int
    free_list_length: int
    free_list_head: int
    root: int
    leaf_count: int


@dataclass(slots=True)
class _LeafNode:
    key: int
    quantity: int


@dataclass(slots=True)
class _InnerNode:
    children: Tuple[int, int]


_SlabNode = Tuple[str, Any]


def _parse_slab(data: bytes) -> Tuple[_SlabHeader, List[_SlabNode]]:
    """Parse a Serum/OpenBook slab from account data."""

    if len(data) < 32:
        raise ValueError("slab data too short")
    bump_index = int.from_bytes(data[0:4], "little", signed=False)
    free_list_length = int.from_bytes(data[8:12], "little", signed=False)
    free_list_head = int.from_bytes(data[16:20], "little", signed=False)
    root = int.from_bytes(data[20:24], "little", signed=False)
    leaf_count = int.from_bytes(data[24:28], "little", signed=False)
    header = _SlabHeader(
        bump_index=bump_index,
        free_list_length=free_list_length,
        free_list_head=free_list_head,
        root=root,
        leaf_count=leaf_count,
    )
    nodes: List[_SlabNode] = []
    offset = 32
    for _ in range(bump_index):
        if offset + 72 > len(data):
            break
        tag = int.from_bytes(data[offset : offset + 4], "little", signed=False)
        offset += 4
        if tag == 2:  # leaf node
            key = int.from_bytes(data[offset + 4 : offset + 20], "little", signed=False)
            quantity = int.from_bytes(
                data[offset + 52 : offset + 60], "little", signed=False
            )
            nodes.append(("leaf", _LeafNode(key=key, quantity=quantity)))
        elif tag == 1:  # inner node
            child0 = int.from_bytes(data[offset + 20 : offset + 24], "little", signed=False)
            child1 = int.from_bytes(data[offset + 24 : offset + 28], "little", signed=False)
            nodes.append(("inner", _InnerNode(children=(child0, child1))))
        elif tag in (0, 3, 4):
            nodes.append(("other", None))
        else:
            nodes.append(("other", None))
        offset += 68
    return header, nodes


def _slab_iter(
    header: _SlabHeader, nodes: List[_SlabNode], descending: bool
) -> Iterable[_LeafNode]:
    if header.leaf_count == 0:
        return []
    stack: List[int] = [header.root]
    while stack:
        index = stack.pop()
        if index < 0 or index >= len(nodes):
            continue
        tag, payload = nodes[index]
        if tag == "leaf" and isinstance(payload, _LeafNode):
            yield payload
        elif tag == "inner" and isinstance(payload, _InnerNode):
            first, second = payload.children
            if descending:
                stack.append(first)
                stack.append(second)
            else:
                stack.append(second)
                stack.append(first)


# ---------------------------------------------------------------------------
# Caches and shared state
# ---------------------------------------------------------------------------

_market_cache: Dict[str, Tuple[float, List[Mapping[str, Any]]]] = {}
_token_cache: Tuple[float, Dict[str, int]] = (0.0, {})
_dex_cache: Dict[str, Tuple[float, DexStats]] = {}
_pyth_cache: Dict[str, Tuple[float, PythPrice]] = {}
_pyth_mapping: Optional[Dict[str, Any]] = None
_pyth_mapping_lock = asyncio.Lock()

_synth_state: Dict[str, SyntheticState] = {}
_synth_details: Dict[str, Dict[str, Any]] = {}
_state_locks: Dict[str, asyncio.Lock] = {}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _now() -> float:
    return time.time()


async def _load_market_cache(session: aiohttp.ClientSession) -> Dict[str, List[Any]]:
    now = _now()
    stale_keys = []
    for key, (ts, _) in _market_cache.items():
        if now - ts > _MARKET_CACHE_TTL:
            stale_keys.append(key)
    if stale_keys or not _market_cache:
        async with session.get(_JUP_MARKETS_URL, timeout=15) as resp:
            resp.raise_for_status()
            payload = await resp.json()
        for kind, entries in payload.items():
            if isinstance(entries, list):
                _market_cache[kind] = (now, entries)
    return {kind: entries for kind, (_, entries) in _market_cache.items()}


async def _load_token_decimals(session: aiohttp.ClientSession) -> Dict[str, int]:
    now = _now()
    ts, cache = _token_cache
    if cache and now - ts <= _TOKEN_CACHE_TTL:
        return cache
    async with session.get(_JUP_TOKENS_URL, timeout=30) as resp:
        resp.raise_for_status()
        payload = await resp.json()
    mapping: Dict[str, int] = {}
    if isinstance(payload, list):
        for entry in payload:
            if not isinstance(entry, Mapping):
                continue
            address = entry.get("address")
            decimals = entry.get("decimals")
            if isinstance(address, str) and isinstance(decimals, int):
                mapping[address] = decimals
    _token_cache = (now, mapping)
    return mapping


async def _ensure_pyth_mapping() -> Dict[str, Any]:
    global _pyth_mapping
    if _pyth_mapping is not None:
        return _pyth_mapping
    async with _pyth_mapping_lock:
        if _pyth_mapping is None:
            mapping, _ = _parse_pyth_mapping()
            _pyth_mapping = mapping
    return _pyth_mapping or {}


@dataclass(slots=True)
class _SerumMarket:
    market: str
    base_mint: str
    quote_mint: str
    bids: str
    asks: str
    base_lot_size: int
    quote_lot_size: int


def _parse_serum_market(entry: Mapping[str, Any]) -> Optional[_SerumMarket]:
    try:
        data = entry.get("data")
        if not (isinstance(data, Sequence) and data and isinstance(data[0], str)):
            return None
        raw = base64.b64decode(data[0])
    except Exception:
        return None
    if len(raw) < 376:  # sanity check
        return None
    offset = 5
    offset += 8  # account_flags
    offset += 32  # own address
    offset += 8  # vault nonce
    base_mint = Pubkey.from_bytes(raw[offset : offset + 32]).to_string()
    offset += 32
    quote_mint = Pubkey.from_bytes(raw[offset : offset + 32]).to_string()
    offset += 32
    offset += 32  # base vault
    offset += 8  # base deposits total
    offset += 8  # base fees accrued
    offset += 32  # quote vault
    offset += 8  # quote deposits total
    offset += 8  # quote fees accrued
    offset += 8  # quote dust threshold
    offset += 32  # request queue
    event_queue = Pubkey.from_bytes(raw[offset : offset + 32])
    offset += 32
    bids = Pubkey.from_bytes(raw[offset : offset + 32]).to_string()
    offset += 32
    asks = Pubkey.from_bytes(raw[offset : offset + 32]).to_string()
    offset += 32
    base_lot_size = int.from_bytes(raw[offset : offset + 8], "little", signed=False)
    offset += 8
    quote_lot_size = int.from_bytes(raw[offset : offset + 8], "little", signed=False)
    offset += 8
    market_pk = entry.get("pubkey")
    market = str(market_pk) if isinstance(market_pk, str) else event_queue.to_string()
    return _SerumMarket(
        market=market,
        base_mint=base_mint,
        quote_mint=quote_mint,
        bids=bids,
        asks=asks,
        base_lot_size=base_lot_size,
        quote_lot_size=quote_lot_size,
    )


async def _fetch_account_bytes(client: AsyncClient, address: str) -> Optional[bytes]:
    try:
        resp = await client.get_account_info(Pubkey.from_string(address))
    except Exception:
        return None
    value = getattr(resp, "value", None)
    if not value:
        return None
    data = value.get("data") if isinstance(value, Mapping) else None
    encoded = None
    if isinstance(data, (list, tuple)) and data:
        encoded = data[0]
    elif isinstance(data, str):
        encoded = data
    if not isinstance(encoded, str):
        return None
    try:
        return base64.b64decode(encoded)
    except Exception:
        return None


async def _fetch_pyth_detail(
    session: aiohttp.ClientSession, mint: str
) -> Optional[PythPrice]:
    mapping = await _ensure_pyth_mapping()
    identifier = mapping.get(mint)
    feed_id = None
    if identifier is not None:
        feed_id = getattr(identifier, "feed_id", None) or getattr(identifier, "price_id", None)
    if not isinstance(feed_id, str) or not feed_id:
        return None
    clean_id = feed_id[2:] if feed_id.startswith("0x") else feed_id
    params = {
        "ids[]": clean_id,
        "verbose": "true",
    }
    try:
        async with session.get(_PYTH_HERMES_PRICE_URL, params=params, timeout=10) as resp:
            resp.raise_for_status()
            payload = await resp.json()
    except Exception:
        return None
    if not isinstance(payload, list) or not payload:
        return None
    entry = payload[0]
    price_entry = entry.get("price") if isinstance(entry, Mapping) else None
    if not isinstance(price_entry, Mapping):
        return None
    price_raw = price_entry.get("price")
    conf_raw = price_entry.get("conf")
    expo = price_entry.get("expo")
    publish_time = price_entry.get("publish_time")
    try:
        expo_int = int(expo)
        price = float(price_raw) * (10.0 ** expo_int)
        confidence = float(conf_raw) * (10.0 ** expo_int)
        publish = float(publish_time)
    except Exception:
        return None
    return PythPrice(price=price, confidence=abs(confidence), publish_time=publish)


async def _fetch_dex_stats(
    session: aiohttp.ClientSession, mint: str
) -> Optional[DexStats]:
    now = _now()
    cached = _dex_cache.get(mint)
    if cached and now - cached[0] <= _DEX_CACHE_TTL:
        return cached[1]
    url = f"{_DEXSCREENER_URL.rstrip('/')}/{mint}"
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            payload = await resp.json()
    except Exception:
        return cached[1] if cached else None
    pairs = payload.get("pairs") if isinstance(payload, Mapping) else None
    best_liquidity = -1.0
    best_price = 0.0
    best_pair = None
    venues: List[str] = []
    total_liquidity = 0.0
    total_volume = 0.0
    if isinstance(pairs, list):
        for pair in pairs:
            if not isinstance(pair, Mapping):
                continue
            liquidity = 0.0
            liquidity_info = pair.get("liquidity")
            if isinstance(liquidity_info, Mapping):
                for key in ("usd", "base", "quote"):
                    value = liquidity_info.get(key)
                    if isinstance(value, (int, float)):
                        liquidity = max(liquidity, float(value))
            elif isinstance(liquidity_info, (int, float)):
                liquidity = float(liquidity_info)
            volume_info = pair.get("volume")
            if isinstance(volume_info, Mapping):
                v = volume_info.get("h24")
                if isinstance(v, (int, float)):
                    total_volume += float(v)
            price_field = pair.get("priceUsd") or pair.get("price")
            try:
                price_val = float(price_field)
            except Exception:
                price_val = 0.0
            dex_id = pair.get("dexId")
            pair_address = pair.get("pairAddress")
            if isinstance(dex_id, str):
                venues.append(dex_id)
            total_liquidity += max(liquidity, 0.0)
            if liquidity > best_liquidity and price_val > 0:
                best_liquidity = liquidity
                best_price = price_val
                best_pair = str(pair_address) if isinstance(pair_address, str) else None
    stats = DexStats(
        liquidity_usd=max(total_liquidity, 0.0),
        volume_24h=max(total_volume, 0.0),
        best_price=best_price,
        venues=venues,
        best_pair=best_pair,
    )
    _dex_cache[mint] = (now, stats)
    return stats


def _levels_from_slab(
    header: _SlabHeader,
    nodes: List[_SlabNode],
    *,
    descending: bool,
    base_lot_size: int,
    quote_lot_size: int,
    base_decimals: int,
    quote_price_usd: float,
    max_levels: int = _MAX_LEVELS,
) -> List[Level]:
    if quote_price_usd <= 0:
        return []
    levels: List[Level] = []
    for node in _slab_iter(header, nodes, descending=descending):
        price_lots = node.key >> 64
        size_lots = node.quantity
        if price_lots <= 0 or size_lots <= 0:
            continue
        price_quote = (price_lots * quote_lot_size) / max(base_lot_size, 1)
        price_usd = price_quote * quote_price_usd
        size_base = (size_lots * base_lot_size) / (10 ** max(base_decimals, 0))
        if price_usd <= 0 or size_base <= 0:
            continue
        levels.append(Level(price=price_usd, size_base=size_base, notional_usd=size_base * price_usd))
        if len(levels) >= max_levels:
            break
    return levels


async def _fetch_orderbook(
    markets: Sequence[_SerumMarket],
    price_quotes: Mapping[str, float],
    token_decimals: Mapping[str, int],
    rpc_url: Optional[str],
) -> Optional[OrderbookData]:
    if not markets:
        return None
    rpc = rpc_url or _DEFAULT_RPC_URL
    client = AsyncClient(rpc, timeout=10)
    bids_all: List[Level] = []
    asks_all: List[Level] = []
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    try:
        for market in markets:
            quote_price = price_quotes.get(market.quote_mint, 0.0)
            if quote_price <= 0 and market.quote_mint in _STABLE_MINTS:
                quote_price = 1.0
            if quote_price <= 0:
                continue
            bids_data = await _fetch_account_bytes(client, market.bids)
            asks_data = await _fetch_account_bytes(client, market.asks)
            if not bids_data or not asks_data:
                continue
            try:
                bids_header, bids_nodes = _parse_slab(bids_data[_ORDERBOOK_HEADER_PREFIX:])
                asks_header, asks_nodes = _parse_slab(asks_data[_ORDERBOOK_HEADER_PREFIX:])
            except Exception:
                continue
            base_decimals = token_decimals.get(market.base_mint, 0)
            bids_levels = _levels_from_slab(
                bids_header,
                bids_nodes,
                descending=True,
                base_lot_size=market.base_lot_size,
                quote_lot_size=market.quote_lot_size,
                base_decimals=base_decimals,
                quote_price_usd=quote_price,
            )
            asks_levels = _levels_from_slab(
                asks_header,
                asks_nodes,
                descending=False,
                base_lot_size=market.base_lot_size,
                quote_lot_size=market.quote_lot_size,
                base_decimals=base_decimals,
                quote_price_usd=quote_price,
            )
            if bids_levels:
                best_bid = max(best_bid or 0.0, bids_levels[0].price)
                bids_all.extend(bids_levels)
            if asks_levels:
                best_price = asks_levels[0].price
                best_ask = best_price if best_ask is None else min(best_ask, best_price)
                asks_all.extend(asks_levels)
    finally:
        try:
            await client.close()
        except Exception:
            pass
    if not bids_all and not asks_all:
        return None
    bids_all.sort(key=lambda level: level.price, reverse=True)
    asks_all.sort(key=lambda level: level.price)
    bids_trim = bids_all[: _MAX_LEVELS]
    asks_trim = asks_all[: _MAX_LEVELS]
    total_bids = sum(level.notional_usd for level in bids_trim)
    total_asks = sum(level.notional_usd for level in asks_trim)
    return OrderbookData(
        bids=bids_trim,
        asks=asks_trim,
        total_bid_usd=total_bids,
        total_ask_usd=total_asks,
        best_bid=best_bid,
        best_ask=best_ask,
    )


def _classify_liquidity(liquidity: float) -> Tuple[str, Dict[str, float]]:
    for threshold, fraction, alpha, tau, spread_floor, label in _LIQUIDITY_CLASSES:
        if liquidity >= threshold:
            return label, {
                "fraction": fraction,
                "alpha": alpha,
                "tau": tau,
                "spread_floor": spread_floor,
            }
    return "thin", {
        "fraction": 0.05,
        "alpha": 0.35,
        "tau": 90.0,
        "spread_floor": 65.0,
    }


def _fallback_spread(spread_floor: float, confidence_bps: float) -> float:
    if confidence_bps > 0:
        return max(spread_floor, confidence_bps * 2.0)
    return spread_floor


def _fallback_depth(dex: Optional[DexStats], mid: float) -> float:
    if not dex or dex.liquidity_usd <= 0 or mid <= 0:
        return 0.0
    label, config = _classify_liquidity(dex.liquidity_usd)
    depth = dex.liquidity_usd * config["fraction"]
    if dex.volume_24h > 0 and dex.liquidity_usd > 0:
        turnover = dex.volume_24h / dex.liquidity_usd
        depth *= min(1.6, 0.6 + 0.25 * turnover)
    return depth


def _compute_depth_pct(
    orderbook: Optional[OrderbookData],
    fallback_depth: float,
    mid: float,
) -> Dict[str, float]:
    buckets = {1: 0.0, 2: 0.0, 5: 0.0}
    if orderbook and mid > 0:
        for pct in buckets:
            bid_limit = mid * (1 - pct / 100.0)
            ask_limit = mid * (1 + pct / 100.0)
            total = 0.0
            for level in orderbook.bids:
                if level.price >= bid_limit:
                    total += level.notional_usd
                else:
                    break
            for level in orderbook.asks:
                if level.price <= ask_limit:
                    total += level.notional_usd
                else:
                    break
            buckets[pct] = total
    fallback_unit = fallback_depth * 0.25
    for pct in buckets:
        buckets[pct] = max(buckets[pct], fallback_unit * (pct / 5.0))
    return {str(k): float(v) for k, v in buckets.items()}


def _blend_depth(
    mint: str,
    state: Optional[SyntheticState],
    inputs: SynthInputs,
    now: float,
) -> Tuple[Optional[SyntheticState], float]:
    mid_candidates = [inputs.mid_price_hint]
    if inputs.dex and inputs.dex.best_price > 0:
        mid_candidates.append(inputs.dex.best_price)
    if inputs.orderbook and inputs.orderbook.best_bid and inputs.orderbook.best_ask:
        mid_candidates.append((inputs.orderbook.best_bid + inputs.orderbook.best_ask) / 2)
    mid = next((value for value in mid_candidates if value and value > 0), 0.0)
    if mid <= 0:
        if state is None:
            return None, 0.0
        # Without a price anchor decay the previous depth slowly.
        elapsed = now - state.last_update
        decay = math.exp(-elapsed / max(state.tau, 60.0))
        new_depth = state.depth_usd * decay
        new_state = SyntheticState(
            depth_usd=new_depth,
            last_update=now,
            mid_usd=state.mid_usd,
            spread_bps=state.spread_bps,
            liquidity_class=state.liquidity_class,
            confidence_bps=state.confidence_bps,
            publish_time=state.publish_time,
            depth_pct=state.depth_pct,
            alpha=state.alpha,
            tau=state.tau,
            extras=dict(state.extras),
        )
        return new_state, new_depth - state.depth_usd

    fallback_depth = _fallback_depth(inputs.dex, mid)
    order_depth = 0.0
    spread = None
    depth_pct = _compute_depth_pct(inputs.orderbook, fallback_depth, mid)
    if inputs.orderbook:
        order_depth = inputs.orderbook.total_bid_usd + inputs.orderbook.total_ask_usd
        spread = inputs.orderbook.spread_bps(mid)
    measured = order_depth
    if fallback_depth > 0:
        if measured > 0:
            measured += fallback_depth * 0.35
        else:
            measured = fallback_depth
    previous = state.depth_usd if state else 0.0
    liquidity = inputs.dex.liquidity_usd if inputs.dex else 0.0
    label, config = _classify_liquidity(liquidity)
    alpha = config["alpha"]
    if inputs.orderbook is None:
        alpha = min(alpha, 0.4)
    tau = config["tau"]
    new_depth = alpha * measured + (1.0 - alpha) * previous
    confidence = 0.0
    publish_time = now
    if inputs.pyth:
        if inputs.pyth.price > 0:
            mid = inputs.pyth.price
        publish_time = inputs.pyth.publish_time
        if inputs.pyth.confidence > 0 and inputs.pyth.price > 0:
            confidence = (inputs.pyth.confidence / inputs.pyth.price) * 10_000
        staleness = max(0.0, now - inputs.pyth.publish_time)
        if staleness > tau:
            decay = math.exp(-(staleness - tau) / max(tau, 1.0))
            new_depth *= decay
    if spread is None:
        spread = _fallback_spread(config["spread_floor"], confidence)
    else:
        spread = max(spread, config["spread_floor"], confidence * 2.0 if confidence else 0.0)
    extras = {
        "orderbook_depth_usd": float(order_depth),
        "fallback_depth_usd": float(fallback_depth),
        "dex_liquidity_usd": float(liquidity),
        "dex_volume_24h": float(inputs.dex.volume_24h if inputs.dex else 0.0),
        "pyth_source": inputs.pyth.source if inputs.pyth else None,
    }
    new_state = SyntheticState(
        depth_usd=new_depth,
        last_update=now,
        mid_usd=mid,
        spread_bps=spread,
        liquidity_class=label,
        confidence_bps=confidence,
        publish_time=publish_time,
        depth_pct=depth_pct,
        alpha=alpha,
        tau=tau,
        extras=extras,
    )
    return new_state, new_depth - previous


async def _gather_inputs(
    mint: str,
    rpc_url: Optional[str],
) -> SynthInputs:
    session = await get_session()
    markets_payload = await _load_market_cache(session)
    serum_entries = markets_payload.get("serum", [])
    relevant_markets: List[_SerumMarket] = []
    for entry in serum_entries:
        if not isinstance(entry, Mapping):
            continue
        parsed = _parse_serum_market(entry)
        if parsed and parsed.base_mint == mint:
            relevant_markets.append(parsed)
    tokens_needed = {mint}
    tokens_needed.update(m.quote_mint for m in relevant_markets)
    price_quotes = await fetch_price_quotes_async(tokens_needed)
    price_map = {token: quote.price_usd for token, quote in price_quotes.items() if quote.price_usd > 0}
    token_decimals = await _load_token_decimals(session)
    orderbook = await _fetch_orderbook(relevant_markets, price_map, token_decimals, rpc_url)
    dex_stats = await _fetch_dex_stats(session, mint)
    pyth_detail = await _fetch_pyth_detail(session, mint)
    mid_hint = price_map.get(mint, 0.0)
    if not mid_hint and dex_stats and dex_stats.best_price > 0:
        mid_hint = dex_stats.best_price
    return SynthInputs(
        pyth=pyth_detail,
        orderbook=orderbook,
        dex=dex_stats,
        mid_price_hint=mid_hint,
    )


def get_cached_snapshot(mint: str) -> Optional[Dict[str, Any]]:
    """Return the most recent synthetic depth metadata for ``mint``."""

    canonical = canonical_mint(mint)
    if not canonical:
        return None
    state = _synth_state.get(canonical)
    if state is None:
        return None
    payload = {
        "depth_usd": state.depth_usd,
        "mid_usd": state.mid_usd,
        "spread_bps": state.spread_bps,
        "liquidity_class": state.liquidity_class,
        "confidence_bps": state.confidence_bps,
        "publish_time": state.publish_time,
        "depth_pct": dict(state.depth_pct),
        "alpha": state.alpha,
        "tau": state.tau,
    }
    payload.update(state.extras)
    return payload


async def compute_depth_change(
    mint: str,
    rpc_url: Optional[str] = None,
) -> float:
    """Compute the depth delta for ``mint`` using live + synthetic inputs."""

    canonical = canonical_mint(mint)
    if not canonical:
        return 0.0
    lock = _state_locks.setdefault(canonical, asyncio.Lock())
    async with lock:
        state = _synth_state.get(canonical)
        inputs = await _gather_inputs(canonical, rpc_url)
        new_state, delta = _blend_depth(canonical, state, inputs, _now())
        if new_state is not None:
            _synth_state[canonical] = new_state
            _synth_details[canonical] = get_cached_snapshot(canonical) or {}
        return float(delta)

