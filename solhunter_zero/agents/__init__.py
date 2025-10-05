from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Type
import importlib
import importlib.metadata
import logging

from ..portfolio import Portfolio
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

_MEMORY_DEPENDENT_AGENT_NAMES = [
    "memory",
    "reinforcement",
    "dqn",
    "ppo",
    "sac",
    "rl_weight",
    "opportunity_cost",
    "inferna",
]

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


BUILT_IN_AGENTS: Dict[str, Type[BaseAgent]] = {}


def _ensure_agents_loaded() -> None:
    if BUILT_IN_AGENTS:
        return
    from .simulation import SimulationAgent
    from .conviction import ConvictionAgent
    from .arbitrage import ArbitrageAgent
    from .exit import ExitAgent
    from .execution import ExecutionAgent
    memory_agent_cls: Type[BaseAgent] | None
    try:
        from .memory import MemoryAgent as _MemoryAgent
    except ModuleNotFoundError as exc:
        missing = exc.name or "unknown"
        logger.warning(
            "Skipping memory-dependent agents (%s) because optional dependency '%s' could not be imported (%s).",
            ", ".join(_MEMORY_DEPENDENT_AGENT_NAMES),
            missing,
            exc,
        )
        memory_agent_cls = None
    else:
        memory_agent_cls = _MemoryAgent
    from .discovery import DiscoveryAgent
    from .portfolio_agent import PortfolioAgent
    from .portfolio_manager import PortfolioManager
    from .portfolio_optimizer import PortfolioOptimizer
    from .crossdex_rebalancer import CrossDEXRebalancer
    from .crossdex_arbitrage import CrossDEXArbitrage
    from .hedging_agent import HedgingAgent
    from .emotion_agent import EmotionAgent
    from .trend import TrendAgent
    from .smart_discovery import SmartDiscoveryAgent

    from .ramanujan_agent import RamanujanAgent
    from .strange_attractor import StrangeAttractorAgent
    from .meta_conviction import MetaConvictionAgent
    from .alien_cipher_agent import AlienCipherAgent
    from .momentum import MomentumAgent
    from .mempool_sniper import MempoolSniperAgent
    from .mev_sandwich import MEVSandwichAgent
    from .flashloan_sandwich import FlashloanSandwichAgent
    from .llm_reasoner import LLMReasoner
    from .artifact_math_agent import ArtifactMathAgent
    from .hierarchical_rl_agent import HierarchicalRLAgent

    # Load legacy compatibility shims so old import paths remain valid.
    _legacy_shims = (
        "config",
        "dex_config",
        "http",
        "order_book_ws",
        "prices",
        "token_discovery",
        "token_scanner",
        "dynamic_limit",
        "resource_monitor",
        "system",
        "onchain_metrics",
        "scanner_common",
        "exchange",
        "util",
    )

    for _shim_name in _legacy_shims:
        importlib.import_module(f"{__name__}.{_shim_name}")

    BUILT_IN_AGENTS.update({
        "simulation": SimulationAgent,
        "conviction": ConvictionAgent,
        "arbitrage": ArbitrageAgent,
        "exit": ExitAgent,
        "execution": ExecutionAgent,
        "discovery": DiscoveryAgent,
        "portfolio": PortfolioAgent,
        "portfolio_manager": PortfolioManager,
        "portfolio_optimizer": PortfolioOptimizer,
        "hedging": HedgingAgent,
        "crossdex_rebalancer": CrossDEXRebalancer,
        "crossdex_arbitrage": CrossDEXArbitrage,
        "trend": TrendAgent,
        "smart_discovery": SmartDiscoveryAgent,

        "momentum": MomentumAgent,
        "mempool_sniper": MempoolSniperAgent,
        "mev_sandwich": MEVSandwichAgent,
        "flashloan_sandwich": FlashloanSandwichAgent,

        "meta_conviction": MetaConvictionAgent,

        "ramanujan": RamanujanAgent,
        "vanta": StrangeAttractorAgent,
        "alien_cipher": AlienCipherAgent,
        "artifact_math": ArtifactMathAgent,
        "hierarchical_rl": HierarchicalRLAgent,

        "llm_reasoner": LLMReasoner,

        "emotion": EmotionAgent,

    })

    if memory_agent_cls is not None:
        from .reinforcement import ReinforcementAgent
        from .opportunity_cost import OpportunityCostAgent
        from .dqn import DQNAgent
        from .ppo_agent import PPOAgent
        from .sac_agent import SACAgent
        from .rl_weight_agent import RLWeightAgent
        from .fractal_agent import FractalAgent

        BUILT_IN_AGENTS.update(
            {
                "memory": memory_agent_cls,
                "reinforcement": ReinforcementAgent,
                "opportunity_cost": OpportunityCostAgent,
                "dqn": DQNAgent,
                "ppo": PPOAgent,
                "sac": SACAgent,
                "rl_weight": RLWeightAgent,
                "inferna": FractalAgent,
            }
        )

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
