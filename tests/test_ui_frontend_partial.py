"""Frontend smoke tests for the dashboard update loop."""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap

import pytest

from solhunter_zero.ui import UIState, create_app


def _extract_module_script(html: str) -> str:
    marker = '<script type="module">'
    start = html.index(marker) + len(marker)
    end = html.index('</script>', start)
    return html[start:end]


def _render_dashboard_module_script(state: UIState) -> str:
    app = create_app(state=state)
    client = app.test_client()
    html = client.get("/").data.decode("utf-8")
    return _extract_module_script(html)


def _simulate_dashboard_refreshes(module_script: str, scenarios: list[dict[str, object]]) -> list[dict[str, object]]:
    node_script = textwrap.dedent(
        f"""
        const vm = require('vm');

        const scenarios = {json.dumps(scenarios)};
        let scenarioIndex = 0;

        const elementStore = new Map();
        function createElement(name) {{
            return {{
                name,
                innerHTML: '',
                textContent: '',
                dataset: {{}},
                style: {{ display: '' }},
                classList: {{
                    add() {{}},
                    remove() {{}},
                }},
                setAttribute() {{}},
                removeAttribute() {{}},
                querySelectorAll() {{ return []; }},
                querySelector() {{ return null; }},
                addEventListener() {{}},
                getContext() {{ return {{}}; }},
            }};
        }}

        const document = {{
            hidden: false,
            querySelector(selector) {{
                if (!elementStore.has(selector)) {{
                    elementStore.set(selector, createElement(selector));
                }}
                return elementStore.get(selector);
            }},
            querySelectorAll() {{
                return [];
            }},
            getElementById(id) {{
                const key = `#${{id}}`;
                if (!elementStore.has(key)) {{
                    const el = createElement(key);
                    if (id === 'actionsChart' || id === 'latencyChart' || id === 'weightsChart') {{
                        el.getContext = () => ({{}});
                    }}
                    elementStore.set(key, el);
                }}
                return elementStore.get(key);
            }},
            addEventListener() {{}},
        }};

        const windowObj = {{
            document,
            addEventListener() {{}},
            Chart: class {{
                constructor() {{
                    this.data = {{ labels: [], datasets: [
                        {{ data: [], hidden: false }},
                        {{ data: [], hidden: false }},
                        {{ data: [], hidden: false }},
                    ] }};
                }}
                update() {{}}
                destroy() {{}}
            }},
            sessionStorage: {{
                _store: new Map(),
                getItem(key) {{ return this._store.has(key) ? this._store.get(key) : null; }},
                setItem(key, value) {{ this._store.set(key, String(value)); }},
                removeItem(key) {{ this._store.delete(key); }},
            }},
            __solhunterAutoRefresh: false,
        }};

        global.window = windowObj;
        global.document = document;
        global.navigator = {{ userAgent: 'node' }};
        global.Chart = windowObj.Chart;
        global.console = console;
        global.setInterval = () => 1;
        global.clearInterval = () => {{}};

        global.fetch = async (path) => {{
            const response = scenarios[scenarioIndex][path];
            if (!response) {{
                throw new Error(`Missing mock for ${{path}} in scenario ${{scenarioIndex}}`);
            }}
            if (response.ok === false) {{
                return {{
                    ok: false,
                    status: response.status ?? 500,
                    json: async () => response.body ?? null,
                }};
            }}
            return {{
                ok: true,
                status: 200,
                json: async () => response.body,
            }};
        }};

        const moduleSource = {json.dumps(module_script)};
        vm.runInContext(moduleSource, vm.createContext(global));

        const hooks = window.__solhunterDashboardTest;
        if (!hooks) {{
            throw new Error('Dashboard test hooks were not initialised');
        }}

        async function runScenario(index) {{
            scenarioIndex = index;
            await hooks.refresh();
            return {{
                data: hooks.getCurrentData(),
                errors: hooks.getEndpointErrors(),
                outcome: hooks.getLastRefreshOutcome(),
            }};
        }}

        (async () => {{
            const results = [];
            for (let i = 0; i < scenarios.length; i += 1) {{
                results.push(await runScenario(i));
            }}
            process.stdout.write(JSON.stringify(results));
        }})().catch(err => {{
            console.error(err);
            process.exit(1);
        }});
        """
    )

    proc = subprocess.run(
        ["node", "-"],
        input=node_script,
        text=True,
        capture_output=True,
        check=True,
    )

    return json.loads(proc.stdout)


def _build_dashboard_refresh_test_inputs() -> tuple[str, list[dict[str, object]], dict[str, object], dict[str, object]]:
    initial_summary = {
        "timestamp": "2024-01-01T00:00:00Z",
        "elapsed_s": 4,
        "actions_count": 1,
        "any_trade": False,
        "discovered_count": 1,
        "picked_tokens": "INIT",
        "committed": False,
        "token_results": [
            {"token": "INIT", "actions": ["seed"], "errors": [], "score": 0.5},
        ],
    }

    state = UIState(
        status_provider=lambda: {
            "trading_loop": False,
            "iterations_completed": 1,
            "dashboard_metrics": {
                "counts": {key: 0 for key in ["activity", "trades", "logs", "weights", "actions"]},
                "stat_tile_map": {},
            },
        },
        summary_provider=lambda: dict(initial_summary),
        discovery_provider=lambda: {"recent": ["INIT"]},
        actions_provider=lambda: [{"agent": "init", "token": "INIT", "side": "hold", "amount": "0"}],
        activity_provider=lambda: [{"event": "init"}],
        trades_provider=lambda: [{"token": "INIT", "side": "buy"}],
        weights_provider=lambda: {"init": 1.0},
        logs_provider=lambda: [{"timestamp": "2024-01-01T00:00:00Z", "payload": {"stage": "init", "detail": "boot"}}],
        config_provider=lambda: {
            "agents": ["init"],
            "loop_delay": 1,
            "min_delay": 1,
            "max_delay": 2,
            "config_path": "/tmp/init.toml",
        },
        history_provider=lambda: [],
    )

    module_script = _render_dashboard_module_script(state)

    summary_success = {
        "timestamp": "2024-01-01T00:05:00Z",
        "elapsed_s": 6,
        "actions_count": 3,
        "any_trade": True,
        "discovered_count": 5,
        "picked_tokens": "AAA,BBB",
        "committed": True,
        "telemetry": {
            "evaluation": {"workers": 2, "latency_avg": 0.5, "latency_max": 0.9, "completed": 7},
            "execution": {"lanes": {"alpha": 1}, "submitted": 2, "lane_workers": 1},
            "pipeline": {"queued": 3, "limit": 10},
        },
        "errors": ["minor warning"],
        "token_results": [
            {"token": "AAA", "actions": [1, 2], "errors": [], "score": 1.23},
            {"token": "BBB", "actions": [3], "errors": ["slow"], "score": 0.5},
        ],
    }

    scenarios = [
        {
            "/status": {
                "ok": True,
                "body": {
                    "trading_loop": True,
                    "iterations_completed": 10,
                    "dashboard_metrics": {
                        "counts": {
                            "activity": 7,
                            "trades": 1,
                            "logs": 1,
                            "weights": 1,
                            "actions": 4,
                        },
                        "stat_tile_map": {
                            "heartbeat": {"value": "60", "caption": "loop"},
                        },
                    },
                },
            },
            "/summary": {"ok": False, "status": 500},
            "/tokens": {"ok": True, "body": {"recent": ["AAA", "BBB"], "latest_iteration_tokens": ["AAA"]}},
            "/actions": {
                "ok": True,
                "body": [
                    {"agent": "alpha", "token": "AAA", "side": "buy", "amount": "1", "result": "ok"},
                ],
            },
            "/activity": {
                "ok": True,
                "body": {"entries": [{"timestamp": "2024-01-01T00:05:00Z", "event": "sample"}]},
            },
            "/trades": {
                "ok": True,
                "body": [
                    {"token": "AAA", "side": "buy", "amount": "1"},
                ],
            },
            "/weights": {"ok": True, "body": {"alpha": 0.7, "beta": 0.3}},
            "/logs": {
                "ok": True,
                "body": {
                    "entries": [
                        {"timestamp": "2024-01-01T00:05:00Z", "payload": {"stage": "loop", "detail": "step"}},
                    ]
                },
            },
            "/config": {
                "ok": True,
                "body": {
                    "agents": ["alpha", "beta"],
                    "loop_delay": 2,
                    "min_delay": 1,
                    "max_delay": 5,
                    "config_path": "/tmp/config.toml",
                },
            },
        },
        {
            "/status": {
                "ok": True,
                "body": {
                    "trading_loop": True,
                    "iterations_completed": 11,
                    "dashboard_metrics": {
                        "counts": {
                            "activity": 9,
                            "trades": 2,
                            "logs": 2,
                            "weights": 1,
                            "actions": 5,
                        },
                        "stat_tile_map": {
                            "heartbeat": {"value": "61", "caption": "loop"},
                        },
                    },
                },
            },
            "/summary": {"ok": True, "body": summary_success},
            "/tokens": {"ok": True, "body": {"recent": ["AAA", "BBB", "CCC"], "latest_iteration_tokens": ["BBB"]}},
            "/actions": {
                "ok": True,
                "body": [
                    {"agent": "alpha", "token": "AAA", "side": "buy", "amount": "1", "result": "ok"},
                    {"agent": "beta", "token": "BBB", "side": "sell", "amount": "2", "result": "ok"},
                ],
            },
            "/activity": {
                "ok": True,
                "body": {"entries": [{"timestamp": "2024-01-01T00:06:00Z", "event": "sample"}]},
            },
            "/trades": {
                "ok": True,
                "body": [
                    {"token": "AAA", "side": "buy", "amount": "1"},
                    {"token": "BBB", "side": "sell", "amount": "2"},
                ],
            },
            "/weights": {"ok": True, "body": {"alpha": 0.6, "beta": 0.4}},
            "/logs": {
                "ok": True,
                "body": {
                    "entries": [
                        {"timestamp": "2024-01-01T00:06:00Z", "payload": {"stage": "loop", "detail": "step"}},
                        {"timestamp": "2024-01-01T00:06:30Z", "payload": {"stage": "loop", "detail": "done"}},
                    ]
                },
            },
            "/config": {
                "ok": True,
                "body": {
                    "agents": ["alpha", "beta"],
                    "loop_delay": 2,
                    "min_delay": 1,
                    "max_delay": 5,
                    "config_path": "/tmp/config.toml",
                },
            },
        },
    ]

    return module_script, scenarios, initial_summary, summary_success


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is required for frontend smoke test")
def test_dashboard_refresh_recovers_from_partial_failures() -> None:
    module_script, scenarios, initial_summary, summary_success = _build_dashboard_refresh_test_inputs()
    results = _simulate_dashboard_refreshes(module_script, scenarios)
    first, second = results

    assert first["data"]["status"]["iterations_completed"] == 10
    assert first["data"]["summary"]["timestamp"] == initial_summary["timestamp"]
    assert first["errors"]["summary"] == "Summary update failed: /summary responded with 500"
    assert first["outcome"]["partial"] is True
    assert first["outcome"]["successCount"] == 8
    assert first["outcome"]["errorCount"] == 1

    assert second["data"]["summary"]["timestamp"] == summary_success["timestamp"]
    assert second["errors"]["summary"] == ""
    assert second["outcome"]["partial"] is False
    assert second["outcome"]["successCount"] == 9
    assert second["outcome"]["errorCount"] == 0


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is required for frontend smoke test")
def test_dashboard_refresh_tracks_successful_sections() -> None:
    module_script, scenarios, initial_summary, _ = _build_dashboard_refresh_test_inputs()
    results = _simulate_dashboard_refreshes(module_script, scenarios)
    first, second = results

    expected_endpoints = {
        "status",
        "summary",
        "tokens",
        "actions",
        "activity",
        "trades",
        "weights",
        "logs",
        "config",
    }

    assert set(first["outcome"]["successfulEndpoints"]) == expected_endpoints - {"summary"}
    assert set(first["outcome"]["failedEndpoints"]) == {"summary"}
    assert first["outcome"]["success"] is True
    assert first["outcome"]["partial"] is True
    assert first["data"]["summary"]["timestamp"] == initial_summary["timestamp"]
    assert all(
        message == ""
        for key, message in first["errors"].items()
        if key != "summary"
    )

    assert set(second["outcome"]["successfulEndpoints"]) == expected_endpoints
    assert set(second["outcome"]["failedEndpoints"]) == set()
    assert second["outcome"]["success"] is True
    assert second["outcome"]["partial"] is False
