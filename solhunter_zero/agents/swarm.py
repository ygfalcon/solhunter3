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
        self._last_outcomes: Dict[str, bool | None] = {a.name: None for a in self.agents}
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
        if not self.memory:
            return
        by_agent: Dict[str, bool] = {}
        for action, res in zip(self._last_actions, results):
            name = action.get("agent")
            token = action.get("token")
            if not name or not token:
                continue
            ok = bool(res.get("ok", False))
            by_agent[name] = ok
            expected = float(action.get("expected_roi", 0.0))
            prob = 1.0 if ok else 0.0
            self.memory.log_simulation(
                token,
                expected_roi=expected,
                success_prob=prob,
                agent=name,
            )
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
    ) -> List[Dict[str, Any]]:
        """Gather proposals from all agents and return aggregated actions.

        Parameters
        ----------
        weights:
            Optional mapping of agent name to weighting factor applied to the
            amounts returned by that agent. Defaults to ``1.0`` for all agents.
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
            try:
                coro = agent.propose_trade(token, portfolio, **kwargs)
                if self._agent_timeout:
                    return await asyncio.wait_for(coro, timeout=self._agent_timeout)
                return await coro
            except asyncio.TimeoutError:
                logger.warning("Agent %s timed out for %s (%.2fs)", agent.name, token, self._agent_timeout)
                return []

        raw_results = await asyncio.gather(*(run(a) for a in self.agents), return_exceptions=True)
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
                m = merged.setdefault(key, {"token": token, "side": side, "amount": 0.0, "price": 0.0})
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
                final.append(entry)
            elif net < 0:
                entry = {"token": tok, "side": "sell", "amount": -net, "price": sell["price"]}
                for extra in ("conviction_delta", "regret", "misfires", "agent", "bias"):
                    if extra in sell:
                        entry[extra] = sell[extra]
                final.append(entry)

        self._last_actions = list(final)
        return final
