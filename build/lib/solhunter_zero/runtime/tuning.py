from __future__ import annotations

from typing import Any, Dict, List

_AGENT_HINTS = {
    "simulation": (
        "Increase AGENT_TIMEOUT or relax thresholds (STOP_LOSS, TAKE_PROFIT, MAX_DRAWDOWN) "
        "if simulations consistently return no trades."
    ),
    "momentum": (
        "Ensure price history is available and consider adjusting momentum thresholds if "
        "no momentum trades are proposed."
    ),
    "trend": (
        "Trend agent relies on sustained price movement. Verify discovery feeds higher-volume "
        "tokens or reduce trend sensitivity."
    ),
    "arbitrage": (
        "Arbitrage agent needs depth data. Confirm depth_service is running and the token has "
        "sufficient DEX liquidity."
    ),
    "conviction": (
        "Conviction agent weighs long-term metrics. Consider relaxing conviction thresholds "
        "if it never proposes trades."
    ),
    "exit": (
        "Exit agent acts on existing positions. No proposals often means no open position or "
        "stop/take-profit thresholds are tight."
    ),
}


def _default_hint(agent: str) -> str:
    return _AGENT_HINTS.get(
        agent,
        "Agent returned no trades; review its configuration thresholds or data inputs.",
    )


def analyse_evaluation(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build actionable per-agent diagnostics from the evaluation metadata emitted by
    `AgentManager`.
    """

    agents = metadata.get("agents")
    swarm = metadata.get("swarm", {})
    agent_meta = swarm.get("agents", [])
    results: List[Dict[str, Any]] = []

    if not agent_meta:
        return results

    for detail in agent_meta:
        agent = str(detail.get("agent", "unknown"))
        proposals = int(detail.get("proposals", 0))
        weight_raw = detail.get("weight")
        try:
            weight_val = float(weight_raw) if weight_raw is not None else None
        except (TypeError, ValueError):
            weight_val = None
        entry: Dict[str, Any] = {
            "agent": agent,
            "proposals": proposals,
            "weight": weight_val,
        }
        if "role" in detail:
            entry["role"] = detail.get("role")
        if "global_share" in detail:
            value = detail.get("global_share")
            try:
                entry["global_share"] = float(value) if value is not None else None
            except (TypeError, ValueError):
                entry["global_share"] = None
        else:
            entry["global_share"] = None
        if "role_share" in detail:
            value = detail.get("role_share")
            try:
                entry["role_share"] = float(value) if value is not None else None
            except (TypeError, ValueError):
                entry["role_share"] = None
        else:
            entry["role_share"] = None
        if "metrics" in detail:
            raw_metrics = detail.get("metrics") or {}
            metrics_converted: Dict[str, Any] = {}
            for key, value in raw_metrics.items():
                try:
                    metrics_converted[key] = float(value) if value is not None else None
                except (TypeError, ValueError):
                    metrics_converted[key] = None
            entry["metrics"] = metrics_converted
        else:
            entry["metrics"] = {}
        if "requirements" in detail:
            entry["requirements"] = detail.get("requirements") or []
        else:
            entry["requirements"] = []
        if "blocked_reason" in detail:
            entry["blocked_reason"] = detail.get("blocked_reason")
        if "needs_attention" in detail:
            entry["needs_attention"] = detail.get("needs_attention")
        if "simulation" in detail:
            entry["simulation"] = detail.get("simulation") or {}
        if "sides" in detail:
            entry["sides"] = detail.get("sides") or []

        if "error" in detail:
            entry["status"] = "error"
            entry["message"] = f"{detail['error']}"
        elif proposals <= 0:
            entry["status"] = "no-trade"
            entry["message"] = _default_hint(agent)
        else:
            entry["status"] = "proposed"
            samples = detail.get("sample") or []
            metrics = detail.get("metrics") or {}
            message_parts = [f"{proposals} proposal(s)"]
            avg_roi = metrics.get("avg_roi")
            if isinstance(avg_roi, (int, float)):
                message_parts.append(f"avg ROI {avg_roi:.3f}")
            avg_success = metrics.get("avg_success")
            if isinstance(avg_success, (int, float)):
                message_parts.append(f"avg success {avg_success:.2f}")
            avg_amount = metrics.get("avg_amount")
            if isinstance(avg_amount, (int, float)):
                message_parts.append(f"avg amount {avg_amount:.3f}")
            entry["message"] = "; ".join(message_parts)
            if samples:
                entry["sample"] = samples[0]

        results.append(entry)

    return results
