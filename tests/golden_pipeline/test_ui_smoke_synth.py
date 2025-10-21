import re
from html.parser import HTMLParser

import pytest

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
    assert all((entry.get("liq") or 0.0) > 0.0 for entry in snapshots)

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
