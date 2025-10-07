from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Iterable, List, Dict, Any

from .. import order_book_ws

from . import BaseAgent
from ..portfolio import Portfolio
from ..advanced_memory import AdvancedMemory

logger = logging.getLogger(__name__)


class AgentSwarm:
    """Coordinate multiple agents and aggregate their proposals."""

    def __init__(
        self,
        agents: Iterable[BaseAgent] | None = None,
        *,
        memory: AdvancedMemory | None = None,
        agent_timeout: float | None = None,
    ):
        self.agents: List[BaseAgent] = list(agents or [])
        self.memory = memory
        self._last_outcomes: Dict[str, Dict[str, Any] | None] = {
            a.name: None for a in self.agents
        }
        self._last_actions: List[Dict[str, Any]] = []
        self._agent_timeout = agent_timeout if agent_timeout and agent_timeout > 0 else None
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
        def _as_float(value: Any) -> float | None:
            try:
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

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

        depth, imbalance, _ = order_book_ws.snapshot(token)
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
                if self._agent_timeout:
                    return await asyncio.wait_for(coro, timeout=self._agent_timeout)
                return await coro
            except asyncio.TimeoutError:
                logger.warning("Agent %s timed out for %s (%.2fs)", agent.name, token, self._agent_timeout)
                return []

        raw_results = await asyncio.gather(
            *(run(a) for a in self.agents), return_exceptions=True
        )
        results: List[List[Dict[str, Any]]] = []
        for agent, raw in zip(self.agents, raw_results):
            res: List[Dict[str, Any]]
            if isinstance(raw, Exception):
                logger.warning("Swarm: %s raised %s for %s", agent.name, raw, token)
                res = []
            elif not isinstance(raw, list):
                res = []
            else:
                res = raw
            logger.info(
                "Swarm: %s returned %s proposals for %s",
                agent.name,
                len(res) if isinstance(res, list) else "?",
                token,
            )
            results.append(res)

        weights_map = {a.name: float(weights.get(a.name, 1.0)) for a in self.agents} if weights else {}

        merged: Dict[tuple[str, str], Dict[str, Any]] = {}
        for agent, res in zip(self.agents, results):
            weight = weights_map.get(agent.name, 1.0)
            if not res:
                continue
            for r in res:
                r.setdefault("agent", agent.name)
                token = r.get("token")
                side = r.get("side")
                if not token or not side:
                    continue
                amt = float(r.get("amount", 0.0)) * weight
                price = float(r.get("price", 0.0))
                key = (token, side)
                m = merged.setdefault(
                    key,
                    {
                        "token": token,
                        "side": side,
                        "amount": 0.0,
                        "price": 0.0,
                        "metadata": {},
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
                metadata = m.setdefault("metadata", {})
                if price <= 0:
                    metadata["needs_price"] = True
                    agents = metadata.setdefault("needs_price_agents", set())
                    agents.add(agent.name)
                old_amt = m["amount"]
                if old_amt + amt > 0:
                    m["price"] = (m["price"] * old_amt + price * amt) / (old_amt + amt)
                m["amount"] += amt

        final: List[Dict[str, Any]] = []
        tokens = {t for t, _ in merged.keys()}
        for tok in tokens:
            buy = merged.get((tok, "buy"), {"amount": 0.0, "price": 0.0})
            sell = merged.get((tok, "sell"), {"amount": 0.0, "price": 0.0})
            net = buy["amount"] - sell["amount"]
            if net > 0:
                entry = {"token": tok, "side": "buy", "amount": net, "price": buy["price"]}
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias"):
                    if extra in buy:
                        entry[extra] = buy[extra]
                if "metadata" in buy and buy["metadata"]:
                    meta = dict(buy["metadata"])
                    agents = meta.get("needs_price_agents")
                    if isinstance(agents, set):
                        meta["needs_price_agents"] = sorted(agents)
                    entry["metadata"] = meta
                final.append(entry)
            elif net < 0:
                entry = {"token": tok, "side": "sell", "amount": -net, "price": sell["price"]}
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias"):
                    if extra in sell:
                        entry[extra] = sell[extra]
                if "metadata" in sell and sell["metadata"]:
                    meta = dict(sell["metadata"])
                    agents = meta.get("needs_price_agents")
                    if isinstance(agents, set):
                        meta["needs_price_agents"] = sorted(agents)
                    entry["metadata"] = meta
                final.append(entry)

        self._last_actions = list(final)
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
