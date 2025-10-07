"""Utilities for working with the built-in trading agents.

This package extends its module search path to include the parent
``solhunter_zero`` directory. As a result, imports such as
``solhunter_zero.agents.config`` transparently resolve to the module located at
``solhunter_zero/config.py`` without requiring bespoke shims. New helper
modules can therefore be dropped directly into ``solhunter_zero`` and remain
accessible through the ``solhunter_zero.agents`` namespace.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, Type
import importlib
import importlib.metadata
import logging

from ..portfolio import Portfolio
from typing import TYPE_CHECKING

_agents_parent = Path(__file__).resolve().parent.parent
_agents_parent_str = str(_agents_parent)
if _agents_parent_str not in __path__:
    __path__.append(_agents_parent_str)

if TYPE_CHECKING:  # Imports for type checking only to avoid circular imports
    from .simulation import SimulationAgent
    from .conviction import ConvictionAgent
    from .arbitrage import ArbitrageAgent
    from .exit import ExitAgent
    from .execution import ExecutionAgent
    from .memory import MemoryAgent
    from .discovery import DiscoveryAgent

    from .dqn import DQNAgent
    from .opportunity_cost import OpportunityCostAgent

    from .ramanujan_agent import RamanujanAgent
    from .strange_attractor import StrangeAttractorAgent
    from .meta_conviction import MetaConvictionAgent
    from .ppo_agent import PPOAgent
    from .sac_agent import SACAgent
    from .portfolio_agent import PortfolioAgent
    from .emotion_agent import EmotionAgent
    from .momentum import MomentumAgent
    from .mempool_sniper import MempoolSniperAgent
    from .mev_sandwich import MEVSandwichAgent
    from .flashloan_sandwich import FlashloanSandwichAgent
    from .momentum import MomentumAgent
    from .mempool_sniper import MempoolSniperAgent
    from .mev_sandwich import MEVSandwichAgent
    from .flashloan_sandwich import FlashloanSandwichAgent

    from .alien_cipher_agent import AlienCipherAgent
    from .llm_reasoner import LLMReasoner

    from .llm_reasoner import LLMReasoner

    from .opportunity_cost import OpportunityCostAgent




class BaseAgent(ABC):
    """Abstract trading agent."""

    name: str = "base"

    @abstractmethod
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        """Return proposed trade actions for ``token``."""
        raise NotImplementedError

    def explain_proposal(
        self,
        actions: List[Dict[str, Any]] | None = None,
        token: str | None = None,
        portfolio: Portfolio | None = None,
    ) -> str:
        """Optional explanation of a trade proposal."""
        return ""


logger = logging.getLogger(__name__)

BUILT_IN_AGENTS: Dict[str, Type[BaseAgent]] = {}

_OPTIONAL_DEPENDENCIES = {
    "sqlalchemy": (
        "Memory-based agents require the optional SQLAlchemy dependency. "
        "Install it to enable persistent trade logging."
    ),
}

_AGENT_IMPORTS: Dict[str, tuple[str, str]] = {
    "simulation": ("simulation", "SimulationAgent"),
    "conviction": ("conviction", "ConvictionAgent"),
    "arbitrage": ("arbitrage", "ArbitrageAgent"),
    "exit": ("exit", "ExitAgent"),
    "execution": ("execution", "ExecutionAgent"),
    "memory": ("memory", "MemoryAgent"),
    "discovery": ("discovery", "DiscoveryAgent"),
    "reinforcement": ("reinforcement", "ReinforcementAgent"),
    "portfolio": ("portfolio_agent", "PortfolioAgent"),
    "portfolio_manager": ("portfolio_manager", "PortfolioManager"),
    "portfolio_optimizer": ("portfolio_optimizer", "PortfolioOptimizer"),
    "hedging": ("hedging_agent", "HedgingAgent"),
    "crossdex_rebalancer": ("crossdex_rebalancer", "CrossDEXRebalancer"),
    "crossdex_arbitrage": ("crossdex_arbitrage", "CrossDEXArbitrage"),
    "dqn": ("dqn", "DQNAgent"),
    "ppo": ("ppo_agent", "PPOAgent"),
    "sac": ("sac_agent", "SACAgent"),
    "opportunity_cost": ("opportunity_cost", "OpportunityCostAgent"),
    "trend": ("trend", "TrendAgent"),
    "smart_discovery": ("smart_discovery", "SmartDiscoveryAgent"),
    "momentum": ("momentum", "MomentumAgent"),
    "mempool_sniper": ("mempool_sniper", "MempoolSniperAgent"),
    "mev_sandwich": ("mev_sandwich", "MEVSandwichAgent"),
    "flashloan_sandwich": ("flashloan_sandwich", "FlashloanSandwichAgent"),
    "meta_conviction": ("meta_conviction", "MetaConvictionAgent"),
    "ramanujan": ("ramanujan_agent", "RamanujanAgent"),
    "vanta": ("strange_attractor", "StrangeAttractorAgent"),
    "inferna": ("fractal_agent", "FractalAgent"),
    "alien_cipher": ("alien_cipher_agent", "AlienCipherAgent"),
    "artifact_math": ("artifact_math_agent", "ArtifactMathAgent"),
    "rl_weight": ("rl_weight_agent", "RLWeightAgent"),
    "hierarchical_rl": ("hierarchical_rl_agent", "HierarchicalRLAgent"),
    "llm_reasoner": ("llm_reasoner", "LLMReasoner"),
    "emotion": ("emotion_agent", "EmotionAgent"),
}


def _load_agent_class(agent_name: str, module_name: str, class_name: str) -> Type[BaseAgent] | None:
    try:
        module = importlib.import_module(f".{module_name}", __name__)
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised via tests
        missing_root = (exc.name or "").partition(".")[0]
        if not missing_root:
            text = str(exc)
            if text.startswith("No module named"):
                missing_root = text.split("'", 2)[1]
        message = _OPTIONAL_DEPENDENCIES.get(missing_root)
        if message is not None:
            logger.warning(
                "Skipping agent '%s' because optional dependency '%s' is not installed. %s",
                agent_name,
                missing_root,
                message,
            )
            return None
        raise
    return getattr(module, class_name)


def _ensure_agents_loaded() -> None:
    if BUILT_IN_AGENTS:
        return
    for agent_name, (module_name, class_name) in _AGENT_IMPORTS.items():
        agent_cls = _load_agent_class(agent_name, module_name, class_name)
        if agent_cls is not None:
            BUILT_IN_AGENTS[agent_name] = agent_cls

    for ep in importlib.metadata.entry_points(group="solhunter_zero.agents"):
        try:
            agent_cls = ep.load()
        except Exception:  # pragma: no cover - load errors ignored
            continue
        name = getattr(agent_cls, "name", None) or ep.name
        if isinstance(name, str):
            BUILT_IN_AGENTS[name] = agent_cls


def load_agent(name: str, **kwargs) -> BaseAgent:
    """Instantiate a built-in agent by name.

    Parameters
    ----------
    name:
        The agent name. Must be one of ``BUILT_IN_AGENTS``.

    Returns
    -------
    BaseAgent
        The instantiated agent.

    Raises
    ------
    KeyError
        If ``name`` is not a known agent.
    """
    _ensure_agents_loaded()
    if name not in BUILT_IN_AGENTS:
        raise KeyError(name)
    agent_cls = BUILT_IN_AGENTS[name]
    return agent_cls(**kwargs)
