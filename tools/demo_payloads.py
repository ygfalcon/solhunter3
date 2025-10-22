from __future__ import annotations

import json
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from solhunter_zero.golden_pipeline.types import (
    DepthSnapshot,
    DiscoveryCandidate,
    TapeEvent,
    TokenSnapshot,
)

ARTIFACT_ROOT = Path("artifacts/demo")
ARTIFACT_DIR = ARTIFACT_ROOT / "frames"
REPORT_JSON_PATH = ARTIFACT_ROOT / "report.json"
REPORT_MARKDOWN_PATH = ARTIFACT_ROOT / "report.md"

_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


@dataclass(slots=True)
class DemoToken:
    payload: Dict[str, Any]
    spread_bp: int
    depth_scale: float
    trade_notional: float

    @property
    def mint(self) -> str:
        return str(self.payload["mint"])


def _base58_mint(index: int) -> str:
    random.seed(index + 2024)
    chars = [_BASE58[random.randint(0, len(_BASE58) - 1)] for _ in range(43)]
    prefix = "D" if index % 2 == 0 else "R"
    return prefix + "".join(chars)


def generate_candidates(count: int = 20, *, seed: int = 42) -> List[DemoToken]:
    random.seed(seed)
    base_ts = 1_761_123_456.0
    tokens: List[DemoToken] = []
    for idx in range(count):
        mid = round(random.uniform(0.0015, 0.018), 6)
        notional = round(random.uniform(18_000, 82_000), 2)
        spread_bp = random.randint(30, 85)
        depth_scale = random.uniform(1.2, 2.4)
        trade_notional = round(random.uniform(90.0, 220.0), 2)
        mint = _base58_mint(idx)
        payload = {
            "mint": mint,
            "symbol": f"DEMO{idx:02d}",
            "name": f"Demo Token {idx:02d}",
            "decimals": 6,
            "seen_at": base_ts + idx * 0.5,
            "source": {
                "kind": "synthetic:demo",
                "aliases": ["dexscreener"],
                "uri": f"demo://seed/{idx}",
            },
            "hints": {
                "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "venues": ["ORCA", "RAYDIUM"],
                "creator": f"Cr8Demo{idx:02d}",
            },
            "initial_price": {"mid": mid, "quote": "USDC"},
            "liquidity": {"notional": notional, "quote": "USDC"},
            "risk": {
                "fdv": round(mid * 1_000_000, 2),
                "mint_age_sec": random.randint(90, 540),
                "flags": ["no-mint-authority"],
            },
            "schema_version": 3,
        }
        tokens.append(
            DemoToken(
                payload=payload,
                spread_bp=spread_bp,
                depth_scale=depth_scale,
                trade_notional=trade_notional,
            )
        )
    return tokens


def candidate_to_dataclass(token: DemoToken) -> DiscoveryCandidate:
    payload = token.payload
    source = payload.get("source") or {}
    aliases = tuple(source.get("aliases", []))
    seen_at = float(payload.get("seen_at") or time.time())
    return DiscoveryCandidate(
        mint=str(payload["mint"]),
        asof=seen_at,
        source=source,
        sources=aliases,
        v="3.0",
        symbol=str(payload.get("symbol")),
        name=str(payload.get("name", payload.get("symbol", "")) or ""),
        decimals=int(payload.get("decimals", 6)),
        seen_at=seen_at,
        schema_version=int(payload.get("schema_version", 3)),
        uri=str(source.get("uri") or "") or None,
        hints=dict(payload.get("hints", {})),
        initial_price=dict(payload.get("initial_price", {})),
        liquidity=dict(payload.get("liquidity", {})),
        risk=dict(payload.get("risk", {})),
    )


def build_token_snapshot(token: DemoToken, *, asof: float | None = None) -> TokenSnapshot:
    payload = token.payload
    ts = float(asof or payload.get("seen_at") or time.time())
    return TokenSnapshot(
        mint=str(payload["mint"]),
        symbol=str(payload["symbol"]),
        name=str(payload.get("name", payload["symbol"])),
        decimals=int(payload.get("decimals", 6)),
        token_program=str(payload["hints"].get("program")),
        venues=tuple(payload["hints"].get("venues", [])),
        flags=dict(payload.get("risk", {})),
        asof=ts,
    )


def _depth_profile(base_value: float, scale: float) -> Dict[str, float]:
    depth1 = base_value
    depth2 = depth1 * scale
    depth5 = depth2 * scale
    return {
        "0.5": round(depth1 * 0.6, 2),
        "1": round(depth1, 2),
        "2": round(depth2, 2),
        "5": round(depth5, 2),
    }


def build_depth_snapshots(
    token: DemoToken,
    *,
    base_ts: float,
    venues: Sequence[str] = ("ORCA", "RAYDIUM"),
) -> List[DepthSnapshot]:
    depth_total = float(token.payload["liquidity"]["notional"]) * 0.072
    depth_map = _depth_profile(depth_total, token.depth_scale)
    snapshots: List[DepthSnapshot] = []
    mid = float(token.payload["initial_price"]["mid"])
    spread_bp = float(token.spread_bp)
    for idx, venue in enumerate(venues):
        jitter = 1.0 + (0.0007 * (idx + 1))
        ts = base_ts + (idx * 0.12)
        snapshots.append(
            DepthSnapshot(
                mint=str(token.payload["mint"]),
                venue=venue,
                mid_usd=round(mid * jitter, 6),
                spread_bps=spread_bp,
                depth_pct=dict(depth_map),
                asof=ts,
                px_bid_usd=round(mid * (1 - spread_bp / 20000.0), 6),
                px_ask_usd=round(mid * (1 + spread_bp / 20000.0), 6),
                depth_bands_usd={venue: round(depth_total / 2.0, 2)},
                degraded=False,
                source="synthetic:demo",
                staleness_ms=80.0,
            )
        )
    return snapshots


def build_trade_events(token: DemoToken, *, base_ts: float) -> List[TapeEvent]:
    mid = float(token.payload["initial_price"]["mid"])
    notional = float(token.trade_notional)
    events: List[TapeEvent] = []
    for i in range(2):
        ts = base_ts + i * 0.18
        price = round(mid * (1 + 0.0009 * (i + 1)), 6)
        amount_quote = round(notional * (1 + 0.01 * i), 4)
        amount_base = round(amount_quote / price, 6)
        events.append(
            TapeEvent(
                mint_base=str(token.payload["mint"]),
                mint_quote="USDC",
                amount_base=amount_base,
                amount_quote=amount_quote,
                route="SYNTH-DEMO",
                program_id="DemoProgram111111111111111111111111111111",
                pool=f"DEMO_POOL_{token.payload['symbol']}",
                signer=f"Trader{token.payload['symbol']}",
                signature=f"0xDEMO{token.payload['symbol']}{i:02d}",
                slot=1_234_567 + i,
                ts=ts,
                fees_base=amount_base * 0.0004,
                price_usd=price,
                fees_usd=amount_quote * 0.0004,
                is_self=False,
                buyer=f"buyer_{token.payload['symbol']}_{i}",
            )
        )
    return events


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":")) + "\n")


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    try:
        return float(statistics.median(values))
    except statistics.StatisticsError:
        return 0.0


def _extract_depth(entry: Mapping[str, Any]) -> float:
    depth = entry.get("liq_depth_1pct_usd")
    if depth is not None:
        try:
            return float(depth)
        except (TypeError, ValueError):
            return 0.0
    liq = entry.get("liq")
    if isinstance(liq, Mapping):
        depth_map = liq.get("depth_usd_by_pct") or liq.get("depth_pct")
        if isinstance(depth_map, Mapping):
            for key in ("1", "1.0"):
                if key in depth_map:
                    try:
                        return float(depth_map[key])
                    except (TypeError, ValueError):
                        continue
    return 0.0


def summarise(
    *,
    discovered: Sequence[Mapping[str, Any]],
    golden: Sequence[Mapping[str, Any]],
    suggestions: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    spreads = [float(entry.get("px", {}).get("spread_bps") or 0.0) for entry in golden]
    depth_values = [_extract_depth(entry) for entry in golden]
    edges = [float(entry.get("edge") or 0.0) for entry in suggestions]
    breakeven = [float(entry.get("breakeven_bp") or 0.0) for entry in suggestions]
    top = sorted(
        suggestions,
        key=lambda item: float(item.get("edge") or 0.0),
        reverse=True,
    )[:5]
    summary = {
        "discovery_count": len(discovered),
        "golden_count": len(golden),
        "suggestion_count": len(suggestions),
        "median_spread_bp": _median(spreads),
        "median_depth_1pct": _median(depth_values),
        "median_edge": _median(edges),
        "median_breakeven": _median(breakeven),
        "top_suggestions": [dict(entry) for entry in top],
    }
    return summary


def write_summary_markdown(path: Path, summary: Mapping[str, Any]) -> None:
    status = str(summary.get("status", "UNKNOWN")).upper()
    lines = [
        f"Golden Demo Summary — {status}",
        "",
        f"Discovery: {summary['discovery_count']} candidates (source: synthetic:demo)",
        (
            "Golden:    {count} snapshots (median spread: {spread:.0f} bp, "
            "median depth_1%: ${depth:,.0f})"
        ).format(
            count=summary["golden_count"],
            spread=summary["median_spread_bp"],
            depth=summary["median_depth_1pct"],
        ),
        (
            "Agents:    {count} suggestions (median edge: {edge:.1f}%, "
            "median breakeven: {breakeven:.0f} bp)"
        ).format(
            count=summary["suggestion_count"],
            edge=summary["median_edge"] * 100.0,
            breakeven=summary["median_breakeven"],
        ),
        "",
        "Top 5 by expected edge:",
    ]
    for entry in summary.get("top_suggestions", []):
        mint = str(entry.get("mint"))
        label = mint[:6]
        notional = float(entry.get("notional") or entry.get("notional_usd") or 0.0)
        depth_ref = entry.get("risk", {}).get("depth_1pct_quote")
        if depth_ref:
            depth_value = float(depth_ref)
            depth_text = f"${depth_value/1000:.1f}k" if depth_value >= 1000 else f"${depth_value:,.0f}"
        else:
            depth_text = "$0"
        lines.append(
            f"- {label:<6} {entry.get('side','')}  ${notional:.0f}  edge {float(entry.get('edge', 0.0))*100:.1f}%  "
            f"depth_1% {depth_text}  hash {entry.get('integrity', {}).get('golden_hash', '')}"
        )
    lines.append("")
    checks = summary.get("checks") or {}
    if checks:
        lines.append("Checks:")
        for name, ok in sorted(checks.items()):
            label = name.replace("_", " ")
            state = "PASS" if ok else "FAIL"
            lines.append(f"- {label}: {state}")
        lines.append("")
    metrics = summary.get("metrics") or {}
    if metrics:
        bus = metrics.get("bus_latency_ms")
        depth = metrics.get("depth_latency_ms")
        golden = metrics.get("golden_lag_ms")
        lag_parts = []
        if bus is not None:
            lag_parts.append(f"bus {bus:.0f} ms")
        if depth is not None:
            lag_parts.append(f"depth {depth:.0f} ms")
        if golden is not None:
            lag_parts.append(f"golden {golden:.0f} ms")
        if lag_parts:
            lines.append("Lag metrics: " + " · ".join(lag_parts))
    handshake = summary.get("handshake") or {}
    if handshake:
        lines.append("Handshake:")
        schema = handshake.get("schema_version")
        ack = handshake.get("ack_version")
        if schema is not None:
            lines.append(f"- schema version: {schema}")
        if ack is not None:
            lines.append(f"- ack version: {ack}")
        if handshake.get("event_bus"):
            lines.append(f"- event bus: {handshake['event_bus']}")
        if handshake.get("broker_channel"):
            lines.append(f"- broker channel: {handshake['broker_channel']}")
        lines.append("")
    artifacts = summary.get("artifacts") or {}
    frames_path = artifacts.get("frames")
    report_json = artifacts.get("report_json")
    report_md = artifacts.get("report_markdown")
    lines.append("Artifacts:")
    if frames_path:
        lines.append(f"- frames: {frames_path}/*.jsonl")
    if report_json:
        lines.append(f"- report (json): {report_json}")
    if report_md:
        lines.append(f"- report (markdown): {report_md}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_summary_json(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")


__all__ = [
    "ARTIFACT_ROOT",
    "ARTIFACT_DIR",
    "REPORT_JSON_PATH",
    "REPORT_MARKDOWN_PATH",
    "DemoToken",
    "generate_candidates",
    "candidate_to_dataclass",
    "build_token_snapshot",
    "build_depth_snapshots",
    "build_trade_events",
    "write_jsonl",
    "summarise",
    "write_summary_markdown",
    "write_summary_json",
]
