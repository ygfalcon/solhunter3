from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover - type checking guards
    from . import BaseAgent

RoleLiteral = str

_ROLE_DEFAULT: RoleLiteral = "buyer"
_ROLE_ORDER: tuple[RoleLiteral, ...] = ("buyer", "seller", "helper")

_CLASS_ROLE_HINTS: dict[str, RoleLiteral] = {
    "SimulationAgent": "helper",
    "PortfolioAgent": "helper",
    "PortfolioManager": "helper",
    "PortfolioOptimizer": "helper",
    "MemoryAgent": "helper",
    "EmotionAgent": "helper",
    "RLWeightAgent": "helper",
    "HierarchicalRLAgent": "helper",
    "SupervisorAgent": "helper",
    "ExecutionAgent": "helper",
    "HedgingAgent": "helper",
    "AgentSwarm": "helper",
    "AdvancedMemory": "helper",
    "ConvictionAgent": "buyer",
    "MomentumAgent": "buyer",
    "TrendAgent": "buyer",
    "ArbitrageAgent": "buyer",
    "CrossDEXArbitrage": "buyer",
    "CrossDEXRebalancer": "helper",
    "OpportunityCostAgent": "seller",
    "ExitAgent": "seller",
    "FlashloanSandwichAgent": "buyer",
    "MEVSandwichAgent": "buyer",
    "MempoolSniperAgent": "buyer",
    "RamanujanAgent": "buyer",
    "StrangeAttractorAgent": "buyer",
    "ArtifactMathAgent": "helper",
    "ReinforcementAgent": "buyer",
    "PPOAgent": "buyer",
    "SACAgent": "buyer",
    "DQNAgent": "buyer",
    "MetaConvictionAgent": "buyer",
    "FractalAgent": "buyer",
    "LLMReasoner": "helper",
}

_NAME_ROLE_HINTS: dict[str, RoleLiteral] = {
    "portfolio": "helper",
    "simulator": "helper",
    "simulation": "helper",
    "predictor": "helper",
    "helper": "helper",
    "exit": "seller",
    "liquidate": "seller",
    "seller": "seller",
    "sell": "seller",
    "buyer": "buyer",
    "buy": "buyer",
    "arb": "buyer",
    "arbitrage": "buyer",
    "rebalance": "seller",
    "hedge": "helper",
}


def _normalise_role(value: Any) -> RoleLiteral:
    if isinstance(value, str):
        candidate = value.strip().lower()
        if candidate in _ROLE_ORDER:
            return candidate
    return _ROLE_DEFAULT


def _match_keywords(text: str, keywords: Iterable[str]) -> RoleLiteral | None:
    lowered = text.lower()
    for key in keywords:
        if key in lowered:
            hint = _NAME_ROLE_HINTS.get(key)
            if hint:
                return hint
    return None


def resolve_role_by_name(name: str | None, class_name: str | None = None) -> RoleLiteral:
    if class_name:
        hint = _CLASS_ROLE_HINTS.get(class_name)
        if hint:
            return hint
    for source in (class_name, name):
        if not source:
            continue
        for key, role in _NAME_ROLE_HINTS.items():
            if key in source.lower():
                return role
    return _ROLE_DEFAULT


def resolve_agent_role(agent: "BaseAgent | None") -> RoleLiteral:
    if agent is None:
        return _ROLE_DEFAULT
    role_attr = getattr(agent, "role", None)
    if isinstance(role_attr, str):
        return _normalise_role(role_attr)
    name = getattr(agent, "name", None)
    class_name = agent.__class__.__name__
    return resolve_role_by_name(str(name) if name else None, class_name)


__all__ = [
    "resolve_agent_role",
    "resolve_role_by_name",
]
