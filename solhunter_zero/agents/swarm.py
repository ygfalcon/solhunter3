from __future__ import annotations

import asyncio
import inspect
import logging
import math
from typing import Iterable, List, Dict, Any, Optional, Set

from .. import order_book_ws
from ..logging_utils import serialize_for_log

from . import BaseAgent
from ..portfolio import Portfolio
from ..advanced_memory import AdvancedMemory

logger = logging.getLogger(__name__)


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        if not math.isfinite(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


def _safe_number(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(num):
        return default
    return num


def _summarise_proposals(proposals: List[Dict[str, Any]]) -> Dict[str, float]:
    """Return aggregate statistics for an agent's proposals."""

    if not proposals:
        return {}

    roi_values: List[float] = []
    prob_values: List[float] = []
    amount_values: List[float] = []
    price_values: List[float] = []
    for proposal in proposals:
        roi = _as_float(proposal.get("expected_roi") or proposal.get("roi"))
        if roi is not None:
            roi_values.append(roi)
        prob = _as_float(
            proposal.get("success_prob")
            or proposal.get("probability")
            or proposal.get("confidence")
        )
        if prob is not None:
            prob_values.append(prob)
        amount = _as_float(proposal.get("amount"))
        if amount is not None:
            amount_values.append(amount)
        price = _as_float(proposal.get("price"))
        if price is not None:
            price_values.append(price)

    summary: Dict[str, float] = {"count": float(len(proposals))}
    if roi_values:
        summary["avg_roi"] = sum(roi_values) / len(roi_values)
        summary["max_roi"] = max(roi_values)
        summary["min_roi"] = min(roi_values)
    if prob_values:
        summary["avg_success"] = sum(prob_values) / len(prob_values)
    if amount_values:
        summary["avg_amount"] = sum(amount_values) / len(amount_values)
    if price_values:
        summary["avg_price"] = sum(price_values) / len(price_values)
    return summary


class AgentSwarm:
    """Coordinate multiple agents and aggregate their proposals."""

    def __init__(
        self,
        agents: Iterable[BaseAgent] | None = None,
        *,
        memory: AdvancedMemory | None = None,
        agent_timeout: float | None = None,
        simulation_timeout: float | None = None,
    ):
        self.agents: List[BaseAgent] = list(agents or [])
        self.memory = memory
        self._last_outcomes: Dict[str, Dict[str, Any] | None] = {
            a.name: None for a in self.agents
        }
        self._last_actions: List[Dict[str, Any]] = []
        self._last_proposal_counts: Dict[str, int] = {
            a.name: 0 for a in self.agents
        }
        self._last_total_proposals: int = 0
        self._last_agent_details: List[Dict[str, Any]] = []
        self._agent_timeout = agent_timeout if agent_timeout and agent_timeout > 0 else None
        self._simulation_timeout = (
            simulation_timeout if simulation_timeout and simulation_timeout > 0 else None
        )
        # cache propose_trade parameter names for each agent to avoid repeated
        # inspect.signature calls during execution
        self._param_cache: Dict[str, set[str]] = {}
        for a in self.agents:
            if memory is not None:
                setattr(a, "memory", memory)
            setattr(a, "swarm", self)
            setattr(a, "last_outcome", None)
            try:
                self._param_cache[a.name] = set(inspect.signature(a.propose_trade).parameters)
            except Exception:
                self._param_cache[a.name] = set()

    # ------------------------------------------------------------------
    def success_rate(self, token: str) -> float:
        """Return average recorded success probability for ``token``."""
        if not self.memory:
            return 0.0
        return self.memory.simulation_success_rate(token)

    # ------------------------------------------------------------------
    def record_results(self, results: List[Dict[str, Any]]) -> None:
        """Store execution results and update agent state."""
        by_agent: Dict[str, Dict[str, Any]] = {}
        for action, res in zip(self._last_actions, results):
            if not isinstance(action, dict):
                continue
            res_data = res if isinstance(res, dict) else {}
            name = action.get("agent")
            token = action.get("token")
            if not name or not token:
                continue
            ok = bool(res_data.get("ok", False))
            expected = float(action.get("expected_roi", 0.0))
            realized_roi = _as_float(res_data.get("realized_roi"))
            if realized_roi is None:
                realized_roi = _as_float(action.get("realized_roi"))
            realized_price = _as_float(res_data.get("realized_price"))
            if realized_price is None:
                realized_price = _as_float(action.get("realized_price"))
            outcome: Dict[str, Any] = {
                "ok": ok,
                "expected_roi": expected,
            }
            if realized_roi is not None:
                outcome["realized_roi"] = realized_roi
            if realized_price is not None:
                outcome["realized_price"] = realized_price
            by_agent[name] = outcome
            expected = float(action.get("expected_roi", 0.0))
            prob = 1.0 if ok else 0.0
            if self.memory:
                kwargs = {
                    "expected_roi": expected,
                    "success_prob": prob,
                    "agent": name,
                }
                if realized_roi is not None:
                    kwargs["realized_roi"] = realized_roi
                if realized_price is not None:
                    kwargs["realized_price"] = realized_price
                self.memory.log_simulation(token, **kwargs)
        for agent in self.agents:
            if agent.name in by_agent:
                outcome = by_agent[agent.name]
                self._last_outcomes[agent.name] = outcome
                agent.last_outcome = outcome

    async def propose(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        weights: Dict[str, float] | None = None,
        rl_action: list[float] | None = None,
        regime: str | None = None,
    ) -> List[Dict[str, Any]]:
        """Gather proposals from all agents and return aggregated actions.

        Parameters
        ----------
        weights:
            Optional mapping of agent name to weighting factor applied to the
            amounts returned by that agent. Defaults to ``1.0`` for all agents.
        regime:
            Active market regime detected by :class:`AgentManager`. Agents that
            accept a ``regime`` keyword receive this value so they can adjust
            their decision thresholds accordingly.
        """

        # Snapshot may fail transiently; keep the round alive.
        try:
            depth, imbalance, _ = order_book_ws.snapshot(token)
        except Exception:
            logger.warning("Swarm: order book snapshot failed for %s", token, exc_info=True)
            depth, imbalance = None, None
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Swarm: order book snapshot for %s -> %s",
                token,
                serialize_for_log({"depth": depth, "imbalance": imbalance}),
            )
        summary = None
        if self.memory is not None and hasattr(self.memory, "latest_summary"):
            try:
                summary = self.memory.latest_summary()
            except Exception:
                summary = None

        async def run(agent: BaseAgent):
            logger.info("Swarm: invoking %s for %s", agent.name, token)
            if hasattr(agent, "last_outcome"):
                agent.last_outcome = self._last_outcomes.get(agent.name)
            kwargs = dict(depth=depth, imbalance=imbalance)
            params = self._param_cache.get(agent.name, set())
            if "rl_action" in params:
                kwargs["rl_action"] = rl_action
            if summary is not None and "summary" in params:
                kwargs["summary"] = summary
            if "regime" in params:
                kwargs["regime"] = regime
            try:
                coro = agent.propose_trade(token, portfolio, **kwargs)
                timeout = self._agent_timeout
                if agent.name == "simulation" and self._simulation_timeout is not None:
                    timeout = self._simulation_timeout
                if timeout:
                    return await asyncio.wait_for(coro, timeout=timeout)
                return await coro
            except asyncio.TimeoutError:
                timeout = (
                    self._simulation_timeout
                    if agent.name == "simulation" and self._simulation_timeout is not None
                    else self._agent_timeout
                )
                logger.warning(
                    "Agent %s timed out for %s (%.2fs)",
                    agent.name,
                    token,
                    timeout or 0.0,
                )
                return []

        raw_results = await asyncio.gather(
            *(run(a) for a in self.agents), return_exceptions=True
        )
        results: List[List[Dict[str, Any]]] = []
        proposal_counts: Dict[str, int] = {}
        total_proposals = 0
        agent_details: List[Dict[str, Any]] = []
        for agent, raw in zip(self.agents, raw_results):
            res: List[Dict[str, Any]]
            if isinstance(raw, Exception):
                logger.warning("Swarm: %s raised %s for %s", agent.name, raw, token)
                res = []
            elif not isinstance(raw, list):
                res = []
            else:
                res = raw
            count = len(res) if isinstance(res, list) else 0
            proposal_counts[agent.name] = count
            total_proposals += count
            logger.info(
                "Swarm: %s returned %s proposals for %s",
                agent.name,
                len(res) if isinstance(res, list) else "?",
                token,
            )
            results.append(res)
            detail: Dict[str, Any] = {
                "agent": agent.name,
                "proposals": count,
            }
            if weights and agent.name in weights:
                raw_weight = weights.get(agent.name, 1.0)
                try:
                    w_detail = float(raw_weight)
                except (TypeError, ValueError):
                    w_detail = 1.0
                if not math.isfinite(w_detail) or w_detail < 0.0:
                    w_detail = 0.0
                detail["weight"] = w_detail
            if isinstance(raw, Exception):
                detail["error"] = str(raw)
            elif res:
                detail["sample"] = res[:3]
                metrics = _summarise_proposals(res)
                if metrics:
                    detail["metrics"] = {k: _safe_number(v) for k, v in metrics.items()}
                sides: Set[str] = {
                    str(entry.get("side")).lower()
                    for entry in res
                    if entry.get("side")
                }
                if sides:
                    detail["sides"] = sorted(sides)
            if "metrics" not in detail:
                detail["metrics"] = {
                    "avg_roi": 0.0,
                    "avg_success": 0.0,
                    "count": float(count),
                }
            agent_details.append(detail)

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Swarm: proposal summary for %s -> %s",
                token,
                serialize_for_log(
                    {
                        "agents": agent_details,
                        "total_proposals": total_proposals,
                    }
                ),
            )

        self._last_proposal_counts = proposal_counts
        self._last_total_proposals = total_proposals
        self._last_agent_details = agent_details

        # Sanitize weights (no NaN/negative values)
        weights_map: Dict[str, float] = {}
        if weights:
            for a in self.agents:
                raw_weight = weights.get(a.name, 1.0)
                try:
                    w = float(raw_weight)
                except (TypeError, ValueError):
                    w = 1.0
                if not math.isfinite(w) or w < 0.0:
                    w = 0.0
                weights_map[a.name] = w

        merged: Dict[tuple[str, str], Dict[str, Any]] = {}
        for agent, res in zip(self.agents, results):
            weight = weights_map.get(agent.name, 1.0)
            if not res:
                continue
            for r in res:
                r.setdefault("agent", agent.name)
                r_tok = r.get("token")
                side_raw = r.get("side")
                side = str(side_raw).lower() if side_raw else ""
                if side not in {"buy", "sell"} or not r_tok:
                    continue
                try:
                    amt = float(r.get("amount", 0.0)) * float(weight)
                except (TypeError, ValueError):
                    amt = 0.0
                if not math.isfinite(amt) or amt < 0.0:
                    continue
                try:
                    price = float(r.get("price", 0.0))
                except (TypeError, ValueError):
                    price = 0.0
                if not math.isfinite(price) or price <= 0.0:
                    price = 0.0
                key = (r_tok, side)
                m = merged.setdefault(
                    key,
                    {
                        "token": r_tok,
                        "side": side,
                        "amount": 0.0,
                        "price": 0.0,
                        "metadata": {},
                        "_price_amt": 0.0,
                        "_any_price": False,
                    },
                )
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias"):
                    if extra not in r:
                        continue
                    if extra == "agent":
                        if "agent" in m and m["agent"] != r["agent"]:
                            m.pop("agent", None)
                        elif "agent" not in m:
                            m["agent"] = r["agent"]
                    elif extra not in m:
                        m[extra] = r[extra]
                for flag in ("simulated", "dry_run"):
                    if flag in r:
                        if flag in m:
                            m[flag] = m[flag] or bool(r.get(flag))
                        else:
                            m[flag] = bool(r.get(flag))
                metadata = m.setdefault("metadata", {})
                contributors: Set[str] = metadata.setdefault("contributors", set())
                contributors.add(agent.name)
                if price <= 0.0:
                    agents_np = metadata.setdefault("needs_price_agents", set())
                    agents_np.add(agent.name)
                else:
                    m["_any_price"] = True
                old_amt = m["amount"]
                if old_amt + amt > 0:
                    denom = m["_price_amt"] + (amt if price > 0.0 else 0.0)
                    if denom > 0.0:
                        m["price"] = (m["price"] * m["_price_amt"] + price * amt) / denom
                        m["_price_amt"] = denom
                m["amount"] += amt

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Swarm: merged ledger for %s -> %s",
                token,
                serialize_for_log(
                    {
                        f"{tok}:{side}": data
                        for (tok, side), data in merged.items()
                    }
                ),
            )

        final: List[Dict[str, Any]] = []
        tokens = {t for t, _ in merged.keys()}
        for tok in tokens:
            buy = merged.get((tok, "buy"), {"amount": 0.0, "price": 0.0})
            sell = merged.get((tok, "sell"), {"amount": 0.0, "price": 0.0})
            net = buy["amount"] - sell["amount"]
            if abs(net) < 1e-12:
                continue
            if net > 0:
                entry = {"token": tok, "side": "buy", "amount": net, "price": buy["price"]}
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias", "simulated", "dry_run"):
                    if extra in buy:
                        entry[extra] = buy[extra]
                if "metadata" in buy and buy["metadata"]:
                    meta = dict(buy["metadata"])
                    needs_price = meta.get("needs_price_agents")
                    if isinstance(needs_price, set):
                        meta["needs_price_agents"] = sorted(needs_price)
                    contributors_meta = meta.get("contributors")
                    if isinstance(contributors_meta, set):
                        meta["contributors"] = sorted(contributors_meta)
                    if not buy.get("_any_price", False) and "needs_price_agents" in meta:
                        meta["needs_price"] = True
                    entry["metadata"] = meta
                final.append(entry)
            elif net < 0:
                entry = {"token": tok, "side": "sell", "amount": -net, "price": sell["price"]}
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias", "simulated", "dry_run"):
                    if extra in sell:
                        entry[extra] = sell[extra]
                if "metadata" in sell and sell["metadata"]:
                    meta = dict(sell["metadata"])
                    needs_price = meta.get("needs_price_agents")
                    if isinstance(needs_price, set):
                        meta["needs_price_agents"] = sorted(needs_price)
                    contributors_meta = meta.get("contributors")
                    if isinstance(contributors_meta, set):
                        meta["contributors"] = sorted(contributors_meta)
                    if not sell.get("_any_price", False) and "needs_price_agents" in meta:
                        meta["needs_price"] = True
                    entry["metadata"] = meta
                final.append(entry)

        self._last_actions = list(final)
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "Swarm: net actions for %s -> %s",
                token,
                serialize_for_log(final),
            )
        if not final:
            breakdown = []
            for agent, res in zip(self.agents, results):
                weight = weights_map.get(agent.name, 1.0) if weights_map else 1.0
                count = len(res) if isinstance(res, list) else 0
                breakdown.append(f"{agent.name}(w={weight:.3f}, proposals={count})")
            logger.warning(
                "Swarm: no aggregated actions for %s; breakdown=%s",
                token,
                ", ".join(breakdown) if breakdown else "<no agents>",
            )
        return final

    # ------------------------------------------------------------------
    @property
    def last_proposal_counts(self) -> Dict[str, int]:
        """Return a copy of proposal counts from the most recent run."""

        return dict(self._last_proposal_counts)

    # ------------------------------------------------------------------
    @property
    def last_total_proposals(self) -> int:
        """Return the total number of proposals from the most recent run."""

        return int(self._last_total_proposals)
