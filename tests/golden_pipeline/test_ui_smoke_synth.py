import asyncio
import hashlib
import json
import re
from html.parser import HTMLParser

import pytest

from solhunter_zero.golden_pipeline.contracts import STREAMS
from solhunter_zero.golden_pipeline.types import (
    GOLDEN_SNAPSHOT_SCHEMA_VERSION,
    OHLCV_BAR_SCHEMA_VERSION,
)
from solhunter_zero.golden_pipeline.utils import canonical_hash

pytest_plugins = ["tests.golden_pipeline.synth_seed"]


def test_ui_smoke_synth_values(runtime, bus, kv, synth_seed):
    synth_seed()
    runtime.wait_for_websockets()

    discovery_console = runtime.ui_state.snapshot_discovery_console()
    assert len(discovery_console["candidates"]) >= 5

    golden_snapshot = runtime.ui_state.snapshot_golden_snapshots()
    snapshots = golden_snapshot.get("snapshots", [])
    assert len(snapshots) == 7
    assert all((entry.get("px") or 0.0) > 0.0 for entry in snapshots)
    assert all((entry.get("px_mid_usd") or 0.0) > 0.0 for entry in snapshots)
    assert all((entry.get("liq") or 0.0) > 0.0 for entry in snapshots)
    for entry in snapshots:
        detail = entry.get("px_detail") or {}
        assert (detail.get("mid_usd") or 0.0) > 0.0

    suggestions = runtime.ui_state.snapshot_suggestions()
    assert len(suggestions.get("suggestions", [])) >= 2

    shadow = runtime.ui_state.shadow_provider()
    assert len(shadow.get("virtual_fills", [])) == 1
    assert shadow.get("paper_positions")
    assert shadow["paper_positions"][0]["unrealized_usd"] > 0.0

    token_facts = runtime.ui_state.snapshot_token_facts()
    assert token_facts.get("tokens")

    rl_panel = runtime.ui_state.snapshot_rl_panel()
    assert "weights" in rl_panel

    settings = runtime.ui_state.snapshot_settings()
    assert len(settings.get("controls", [])) >= 3

    status = runtime.ui_state.snapshot_status()
    for key in ("bus_latency_ms", "ohlcv_lag_ms", "depth_lag_ms", "golden_lag_ms"):
        value = status.get(key)
        assert value is not None and 0.0 < value < 500.0
    assert status.get("event_bus") is True
    assert status.get("trading_loop") is True
    assert status.get("loop_state") == "running"

    summary = runtime.summary_snapshot()
    evaluation = summary.get("evaluation", {})
    assert evaluation.get("suggestions_5m") == pytest.approx(3.0, rel=1e-6)
    assert evaluation.get("open_vote_windows") == 2
    assert evaluation.get("acceptance_rate") == pytest.approx(0.667, rel=1e-3)
    golden_meta = summary.get("golden", {})
    assert golden_meta.get("count") == 7
    execution = summary.get("execution", {})
    assert execution.get("count") == 1
    assert execution.get("pnl_1d") == pytest.approx(64.0, rel=1e-3)
    paper_summary = summary.get("paper_pnl", {})
    assert paper_summary.get("latest_unrealized") == pytest.approx(64.0, rel=1e-3)

    collector_summary = runtime.wiring.collectors.summary_snapshot()
    assert "paper_pnl" in collector_summary
    assert "execution" in collector_summary

    vote_state = runtime.ui_state.snapshot_vote_windows()
    assert len(vote_state.get("windows", [])) >= 2

    class ConnectionBadgeParser(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.badges: dict[str, dict[str, str]] = {}
            self._current: str | None = None
            self._depth = 0
            self._collect_detail = False

        def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
            attr_dict = {key: value or '' for key, value in attrs}
            classes = (attr_dict.get('class') or '').split()
            if tag == 'span' and 'connection-badge' in classes:
                channel = attr_dict.get('data-channel') or ''
                if channel:
                    self._current = channel
                    self._depth = 1
                    self._collect_detail = False
                    self.badges[channel] = {
                        'class': attr_dict.get('class', ''),
                        'status': attr_dict.get('data-status', ''),
                        'detail': '',
                    }
                return
            if self._current is not None:
                self._depth += 1
                if tag == 'span' and 'badge-detail' in classes:
                    self._collect_detail = True

        def handle_endtag(self, tag: str) -> None:
            if self._current is None:
                return
            if tag == 'span' and self._collect_detail:
                self._collect_detail = False
            self._depth -= 1
            if self._depth <= 0:
                self._current = None
                self._depth = 0
                self._collect_detail = False

        def handle_data(self, data: str) -> None:
            if self._collect_detail and self._current:
                text = data.strip()
                if text:
                    badge = self.badges[self._current]
                    if badge['detail']:
                        badge['detail'] += ' '
                    badge['detail'] += text

    from solhunter_zero.ui import create_app

    app = create_app(runtime.ui_state)
    with app.test_client() as client:
        response = client.get('/')
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    parser = ConnectionBadgeParser()
    parser.feed(html)
    badges = parser.badges
    expected_channels = {'events', 'market', 'golden', 'agents', 'rl', 'logs'}
    assert expected_channels.issubset(badges.keys())

    metrics_state = suggestions.get('metrics', {})
    agent_age = metrics_state.get('updated_label') or 'n/a'
    agent_expected = ('Stale' if metrics_state.get('stale') else 'Fresh') + f' · {agent_age}'
    assert badges['agents']['detail'] == agent_expected
    if metrics_state.get('stale'):
        assert 'status-warn' in badges['agents']['class']
    else:
        assert 'status-ok' in badges['agents']['class']
    assert 'status-ok' in badges['market']['class'], badges['market']
    assert badges['market']['detail'] == f"Lag {int(round(max(runtime.metrics['ohlcv_lag_ms'], runtime.metrics['depth_lag_ms'])))} ms"
    assert 'status-ok' in badges['golden']['class'], badges['golden']
    assert badges['golden']['detail'] == f"Lag {int(round(runtime.metrics['golden_lag_ms']))} ms"
    assert 'status-pending' in badges['events']['class']
    assert 'status-pending' in badges['rl']['class']
    assert 'status-pending' in badges['logs']['class']

    assert re.search(r'Session Summary', html)
    assert re.search(r'Suggestions / 5m</span>\s*<strong>3\.0', html)
    assert re.search(r'Acceptance Rate</span>\s*<strong>66\.7%', html)
    assert re.search(r'Golden Hashes</span>\s*<strong>7<', html)
    assert re.search(r'Shadow Fills</span>\s*<strong>1<', html)
    assert re.search(r'Open Vote Windows</span>\s*<strong>2<', html)
    assert re.search(r'Paper Unrealized</span>\s*<strong>64\.0<small>USD', html)
    assert re.search(r'Bus Lag</span>\s*<strong>14\.0 ms', html)
    assert re.search(r'OHLCV Lag</span>\s*<strong>220\.0 ms', html)
    assert re.search(r'Depth Lag</span>\s*<strong>180\.0 ms', html)
    assert re.search(r'Golden Lag</span>\s*<strong>95\.0 ms', html)

    golden_panel = re.search(r'<article class="panel" id="golden-panel">(.*?)</article>', html, re.DOTALL)
    assert golden_panel, "expected golden panel in UI"
    golden_rows = re.findall(r'<tr[^>]*data-mint="[^"]+"[^>]*>(.*?)</tr>', golden_panel.group(1), re.DOTALL)
    assert golden_rows, "expected golden snapshot rows"
    for row in golden_rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        assert len(cells) >= 4
        price_text = re.sub(r'<[^>]+>', '', cells[2]).strip()
        liq_text = re.sub(r'<[^>]+>', '', cells[3]).strip()
        assert price_text and price_text != '—'
        assert liq_text and liq_text != '—'


def test_golden_snapshot_nested_payload(runtime, bus):
    mint = "NestedMint111111111111111111111111111111111111111"
    mid_usd = 2.75
    spread_bps = 14.5
    depth_map = {"0.1": 350.0, "0.5": 900.0, "1": 1_500.0, "2": 2_750.0}
    liquidity_total = 5_500.0
    payload = {
        "mint": mint,
        "asof": 1_704_000_000.0,
        "meta": {
            "symbol": "NST",
            "decimals": 6,
            "token_program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "asof": 1_704_000_000.0,
        },
        "px": {"mid_usd": mid_usd, "spread_bps": spread_bps},
        "liq": {
            "depth_pct": depth_map,
            "liquidity_usd": liquidity_total,
            "asof": 1_704_000_000.0,
        },
        "ohlcv5m": {
            "mint": mint,
            "o": 2.7,
            "h": 2.8,
            "l": 2.6,
            "c": 2.75,
            "vol_usd": 12_000.0,
            "trades": 42,
            "buyers": 20,
            "zret": 0.0,
            "zvol": 0.0,
            "asof_close": 1_703_999_700.0,
        },
        "hash": "nested-hash-001",
        "metrics": {"latency_ms": 42.0},
        "schema_version": GOLDEN_SNAPSHOT_SCHEMA_VERSION,
    }

    half_spread = mid_usd * (spread_bps / 20000.0) if mid_usd > 0 else 0.0
    bid_usd = max(0.0, mid_usd - half_spread)
    ask_usd = max(mid_usd, mid_usd + half_spread)
    depth_usd_by_pct = {key: float(value) for key, value in depth_map.items()}
    bands = [{"pct": float(key), "usd": float(value)} for key, value in depth_usd_by_pct.items()]
    ohlcv_snapshot = dict(payload["ohlcv5m"])
    ohlcv_snapshot.pop("mint", None)
    ohlcv_snapshot.update(
        {
            "open": ohlcv_snapshot["o"],
            "high": ohlcv_snapshot["h"],
            "low": ohlcv_snapshot["l"],
            "close": ohlcv_snapshot["c"],
            "volume": ohlcv_snapshot["vol_usd"],
            "volume_usd": ohlcv_snapshot["vol_usd"],
            "volume_base": ohlcv_snapshot["vol_usd"] / mid_usd,
            "schema_version": OHLCV_BAR_SCHEMA_VERSION,
        }
    )
    ohlcv_snapshot["content_hash"] = canonical_hash(
        {key: value for key, value in ohlcv_snapshot.items() if key != "content_hash"}
    )
    payload["ohlcv5m"] = ohlcv_snapshot
    payload["px"].update({"bid_usd": bid_usd, "ask_usd": ask_usd, "ts": payload["asof"]})
    payload["liq"].update(
        {
            "depth_usd_by_pct": depth_usd_by_pct,
            "bands": bands,
            "staleness_ms": 42.0,
            "degraded": False,
            "source": "synthetic",
        }
    )
    payload.update(
        {
            "px_mid_usd": mid_usd,
            "px_bid_usd": bid_usd,
            "px_ask_usd": ask_usd,
            "liq_depth_0_1pct_usd": depth_usd_by_pct["0.1"],
            "liq_depth_0_5pct_usd": depth_usd_by_pct["0.5"],
            "liq_depth_1_0pct_usd": depth_usd_by_pct["1"],
            "liq_depth_1pct_usd": depth_usd_by_pct["1"],
            "degraded": False,
            "source": "synthetic",
            "staleness_ms": 42.0,
        }
    )
    content_source = {
        key: value for key, value in payload.items() if key not in {"content_hash", "idempotency_key"}
    }
    payload["content_hash"] = canonical_hash(content_source)
    idempotency_source = {
        "mint": payload["mint"],
        "meta": payload["meta"],
        "px": payload["px"],
        "liq": payload["liq"],
        "ohlcv5m": payload["ohlcv5m"],
        "schema_version": payload["schema_version"],
    }
    payload["idempotency_key"] = hashlib.sha1(
        json.dumps(idempotency_source, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    asyncio.run(bus.publish(STREAMS.golden_snapshot, payload))
    asyncio.run(runtime.wait_for_golden())

    snapshot = runtime.ui_state.snapshot_golden_snapshots()
    entries = snapshot.get("snapshots", [])
    assert any(entry["mint"] == mint for entry in entries)
    entry = next(item for item in entries if item["mint"] == mint)

    assert entry["px"] == pytest.approx(mid_usd, rel=1e-9)
    assert entry["px_mid_usd"] == pytest.approx(mid_usd, rel=1e-9)
    assert entry["spread_bps"] == pytest.approx(spread_bps, rel=1e-9)
    assert entry["liq"] == pytest.approx(depth_map["1"], rel=1e-9)
    assert entry["liq_total_usd"] == pytest.approx(liquidity_total, rel=1e-9)
    assert entry["liq_depth_pct"]["1"] == pytest.approx(depth_map["1"], rel=1e-9)
    assert entry["liq_depth_pct"]["2"] == pytest.approx(depth_map["2"], rel=1e-9)

    px_detail = entry.get("px_detail") or {}
    assert px_detail.get("spread_bps") == pytest.approx(spread_bps, rel=1e-9)
    liq_detail = entry.get("liq_detail") or {}
    assert liq_detail.get("liquidity_usd") == pytest.approx(liquidity_total, rel=1e-9)
