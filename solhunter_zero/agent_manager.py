from __future__ import annotations

import asyncio
import copy
from .jsonutil import loads, dumps
from .util import run_coro
import os
import random
import inspect
import time
from typing import Iterable, Dict, Any, List, Mapping
from dataclasses import dataclass, field
from .paths import ROOT

from .backtester import backtest_weighted, DEFAULT_STRATEGIES
from .backtest_cli import bayesian_optimize_weights
from .advanced_memory import AdvancedMemory

try:
    import torch
except ImportError:  # pragma: no cover - optional dependency

    class _TorchStub:
        def __getattr__(self, name):
            raise ImportError("torch is required for AgentManager features")

    torch = _TorchStub()  # type: ignore

import logging
import tomllib
from pathlib import Path

from .trade_analyzer import TradeAnalyzer
import numpy as np

from .agents import BaseAgent, load_agent
from .agents.execution import ExecutionAgent
from .execution import EventExecutor
from .agents.swarm import AgentSwarm
from .agents.memory import MemoryAgent
from .agents.emotion_agent import EmotionAgent
from .agents.discovery import DiscoveryAgent
from .swarm_coordinator import SwarmCoordinator
from . import wallet

try:  # optional torch-based swarm
    from .agents.attention_swarm import AttentionSwarm, load_model
except Exception:  # pragma: no cover - torch not available
    AttentionSwarm = None  # type: ignore

    def load_model(*args, **kwargs):  # type: ignore
        raise ImportError("AttentionSwarm requires torch")


from .agents.rl_weight_agent import RLWeightAgent

try:  # Optional hierarchical RL agent (requires torch stack)
    from .agents.hierarchical_rl_agent import HierarchicalRLAgent
except Exception as _hierarchical_import_error:  # pragma: no cover - import guard
    HierarchicalRLAgent = None  # type: ignore[assignment]
else:  # pragma: no cover - executed in environments with deps
    _hierarchical_import_error = None

from .device import get_default_device

try:  # Optional RL supervisor (torch dependency)
    from .hierarchical_rl import SupervisorAgent
except Exception as _supervisor_import_error:  # pragma: no cover
    SupervisorAgent = None  # type: ignore[assignment]
else:  # pragma: no cover
    _supervisor_import_error = None

from .regime import detect_regime
from . import mutation
from .event_bus import publish, subscription
from .schemas import ActionExecuted, WeightsUpdated, RuntimeLog

try:
    from .multi_rl import PopulationRL
except Exception as _population_rl_import_error:  # pragma: no cover
    PopulationRL = None  # type: ignore[assignment]
else:  # pragma: no cover
    _population_rl_import_error = None

try:
    from .rl_training import MultiAgentRL
except Exception as _multi_agent_rl_import_error:  # pragma: no cover
    MultiAgentRL = None  # type: ignore[assignment]
else:  # pragma: no cover
    _multi_agent_rl_import_error = None

from .datasets.sample_ticks import load_sample_ticks, DEFAULT_PATH as _TICKS_PATH
from .config import load_config


logger = logging.getLogger(__name__)


@dataclass
class AgentManagerConfig:
    """Configuration options for :class:`AgentManager`."""

    weights: Dict[str, float] = field(default_factory=dict)
    memory_agent: MemoryAgent | None = None
    emotion_agent: EmotionAgent | None = None
    population_rl: PopulationRL | None = None
    rl_daemon: Any | None = None
    weights_path: str | os.PathLike | None = None
    strategy_selection: bool = False
    vote_threshold: float = 0.0
    mutation_path: str | os.PathLike | None = "mutation_state.json"
    depth_service: bool = False
    priority_rpc: list[str] | None = None
    regime_weights: Dict[str, Dict[str, float]] = field(default_factory=dict)
    decision_thresholds: Dict[str, Dict[str, float]] = field(default_factory=dict)
    evolve_interval: int = 1
    mutation_threshold: float = 0.0
    weight_config_paths: list[str] = field(default_factory=list)
    strategy_rotation_interval: int = 0
    use_attention_swarm: bool = False
    attention_model_path: str | None = None
    use_rl_weights: bool = False
    rl_weights_path: str | None = None
    hierarchical_rl: MultiAgentRL | None = None
    use_supervisor: bool = False
    supervisor_checkpoint: str | None = None
    keypair: Any | None = None
    keypair_path: str | os.PathLike | None = None


class StrategySelector:
    """Rank agents by recent ROI logged by ``MemoryAgent``."""

    def __init__(
        self, memory_agent: MemoryAgent, *, vote_threshold: float = 0.0
    ) -> None:
        self.memory_agent = memory_agent
        self.vote_threshold = float(vote_threshold)

    async def _roi_by_agent(self, names: Iterable[str]) -> Dict[str, float]:
        rois = {n: 0.0 for n in names}
        if not self.memory_agent:
            return rois
        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return rois
        trades = loader(limit=1000)
        if inspect.isawaitable(trades):
            trades = await trades
        summary: Dict[str, Dict[str, float]] = {}
        for t in trades:
            reason = getattr(t, "reason", None)
            if reason not in rois:
                continue
            direction = getattr(t, "direction", "")
            try:
                amount = float(getattr(t, "amount", 0.0))
                price = float(getattr(t, "price", 0.0))
            except Exception:
                continue
            dir_key = str(direction).lower()
            if dir_key not in {"buy", "sell"}:
                continue
            info = summary.setdefault(reason, {"buy": 0.0, "sell": 0.0})
            info[dir_key] += amount * price
        for name in rois:
            info = summary.get(name)
            if not info:
                continue
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                rois[name] = (revenue - spent) / spent
        return rois

    async def rank_agents(self, agents: Iterable[BaseAgent]) -> List[str]:
        names = [a.name for a in agents]
        rois = await self._roi_by_agent(names)
        ranked = sorted(names, key=lambda n: rois.get(n, 0.0), reverse=True)
        return ranked

    async def weight_agents(
        self, agents: Iterable[BaseAgent], base_weights: Dict[str, float]
    ) -> tuple[List[BaseAgent], Dict[str, float]]:
        names = [a.name for a in agents]
        rois = await self._roi_by_agent(names)
        if not rois:
            return list(agents), dict(base_weights)

        ranked = sorted(names, key=lambda n: rois.get(n, 0.0), reverse=True)
        top = ranked[0]
        second_roi = rois.get(ranked[1], 0.0) if len(ranked) > 1 else float("-inf")
        if rois[top] - second_roi >= self.vote_threshold:
            selected = [a for a in agents if a.name == top]
            return selected, {top: base_weights.get(top, 1.0)}

        max_roi = max(rois.values())
        min_roi = min(rois.values())
        if max_roi == min_roi:
            return list(agents), dict(base_weights)

        weights = {}
        for name in names:
            roi = rois.get(name, 0.0)
            norm = (roi - min_roi) / (max_roi - min_roi)
            weights[name] = base_weights.get(name, 1.0) * (1.0 + norm)
        return list(agents), weights


@dataclass
class EvaluationContext:
    """Rich evaluation payload that exposes the originating swarm."""

    token: str
    actions: List[Dict[str, Any]]
    swarm: AgentSwarm | None
    agents: List[BaseAgent]
    weights: Dict[str, float]
    metadata: Dict[str, Any] = field(default_factory=dict)


class AgentManager:
    """Manage and coordinate trading agents and execute actions."""

    def __init__(
        self,
        agents: Iterable[BaseAgent],
        executor: ExecutionAgent | None = None,
        *,
        config: AgentManagerConfig | None = None,
    ):
        cfg = config or AgentManagerConfig()
        self.agents = list(agents)
        exec_kwargs: dict[str, Any] = {
            "depth_service": cfg.depth_service,
            "priority_rpc": cfg.priority_rpc,
        }
        if cfg.keypair is not None:
            exec_kwargs["keypair"] = cfg.keypair
        self.executor = executor or ExecutionAgent(**exec_kwargs)
        if executor is not None and cfg.keypair is not None:
            setattr(self.executor, "keypair", cfg.keypair)
        self.keypair = cfg.keypair
        self.keypair_path = (
            str(cfg.keypair_path)
            if cfg.keypair_path is not None
            else None
        )
        self.weights_path = (
            str(cfg.weights_path) if cfg.weights_path is not None else "weights.json"
        )
        file_weights: Dict[str, float] = {}
        if self.weights_path and os.path.exists(self.weights_path):
            file_weights = self._load_weights(self.weights_path)

        init_weights = cfg.weights or {}
        self.weights = {**file_weights, **init_weights}
        self.regime_weights = cfg.regime_weights or {}
        self.decision_thresholds = copy.deepcopy(cfg.decision_thresholds or {})

        self.memory_agent = cfg.memory_agent or next(
            (a for a in self.agents if isinstance(a, MemoryAgent)),
            None,
        )

        self.emotion_agent = cfg.emotion_agent or next(
            (a for a in self.agents if isinstance(a, EmotionAgent)),
            None,
        )

        self.population_rl = cfg.population_rl
        self.rl_daemon = cfg.rl_daemon

        self.weight_config_paths = list(cfg.weight_config_paths or [])
        self.weight_configs = self._load_weight_configs(self.weight_config_paths)
        self.strategy_rotation_interval = int(cfg.strategy_rotation_interval)
        self.active_weight_config: str | None = None

        self.use_attention_swarm = bool(cfg.use_attention_swarm)
        self.attention_swarm: AttentionSwarm | None = None
        self._attn_history: list[list[float]] = []
        if self.use_attention_swarm and cfg.attention_model_path:
            try:
                attn_device = os.getenv(
                    "ATTENTION_SWARM_DEVICE", str(get_default_device())
                )
                self.attention_swarm = load_model(
                    cfg.attention_model_path, device=attn_device
                )
            except Exception:
                self.attention_swarm = None

        self.use_rl_weights = bool(cfg.use_rl_weights)
        self.rl_weight_agent: RLWeightAgent | None = None
        if self.use_rl_weights:
            self.rl_weight_agent = RLWeightAgent(
                self.memory_agent,
                weights_path=cfg.rl_weights_path or "rl_weights.json",
            )
            self.agents.append(self.rl_weight_agent)

        self.hierarchical_rl = cfg.hierarchical_rl
        self.hierarchical_agent: HierarchicalRLAgent | None = None
        if self.hierarchical_rl is not None:
            if HierarchicalRLAgent is None:
                raise ImportError(
                    "HierarchicalRLAgent requires optional dependencies"
                ) from _hierarchical_import_error
            self.hierarchical_agent = HierarchicalRLAgent(self.hierarchical_rl)
            self.agents.append(self.hierarchical_agent)

        self.supervisor: SupervisorAgent | None = None
        if cfg.use_supervisor:
            if SupervisorAgent is None:
                raise ImportError(
                    "SupervisorAgent requires optional dependencies"
                ) from _supervisor_import_error
            self.supervisor = SupervisorAgent(
                checkpoint=cfg.supervisor_checkpoint or "supervisor.json",
            )

        self.coordinator = SwarmCoordinator(
            self.memory_agent, self.weights, self.regime_weights
        )

        if self.decision_thresholds:
            self._install_threshold_profile(self.decision_thresholds)

        self.mutation_path = (
            str(cfg.mutation_path)
            if cfg.mutation_path is not None
            else "mutation_state.json"
        )
        self.mutation_state = mutation.load_state(self.mutation_path)
        self._restore_mutations()

        self.strategy_selection = cfg.strategy_selection
        self.vote_threshold = float(cfg.vote_threshold)
        self.selector: StrategySelector | None = None
        if self.strategy_selection and self.memory_agent:
            self.selector = StrategySelector(
                self.memory_agent, vote_threshold=self.vote_threshold
            )

        self.evolve_interval = int(cfg.evolve_interval)
        self.mutation_threshold = float(cfg.mutation_threshold)

        self.depth_service = cfg.depth_service
        self._event_executors: Dict[str, EventExecutor] = {}
        self._event_tasks: Dict[str, asyncio.Task] = {}
        self._subscriptions: list[Any] = []

        sub = subscription("config_updated", self._update_from_config)
        sub.__enter__()
        self._subscriptions.append(sub)

        self.rl_policy: Dict[str, float] = {}
        self._fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        timeout_env = os.getenv("AGENT_TIMEOUT")
        if timeout_env:
            try:
                self._agent_timeout = float(timeout_env)
            except Exception:
                self._agent_timeout = None
        elif self._fast_mode:
            self._agent_timeout = 1.0
        else:
            self._agent_timeout = None

        def _merge_rl_weights(payload):
            data = (
                payload.weights
                if hasattr(payload, "weights")
                else payload.get("weights", {})
            )
            if isinstance(data, dict):
                try:
                    self.rl_policy = {str(k): float(v) for k, v in data.items()}
                except Exception:
                    self.rl_policy = {}
                for k, v in data.items():
                    try:
                        self.weights[str(k)] = float(v)
                    except Exception:
                        continue
                self.coordinator.base_weights = self.weights
                self.save_weights()

        rl_sub = subscription("rl_weights", _merge_rl_weights)
        rl_sub.__enter__()
        self._subscriptions.append(rl_sub)

        # Track active swarms keyed by token so concurrent evaluations can
        # feed back execution results without clobbering one another.
        self._active_swarms: Dict[str, AgentSwarm] = {}

    @staticmethod
    def _resolve_keypair_from_config(
        cfg: dict[str, Any] | None,
    ) -> tuple[Any | None, str | None]:
        """Resolve and load the active keypair from configuration/env."""

        candidates: list[str] = []
        if cfg is not None:
            cfg_path = cfg.get("solana_keypair")
            if cfg_path:
                candidates.append(str(cfg_path))

        env_path = os.getenv("SOLANA_KEYPAIR") or os.getenv("solana_keypair")
        if env_path:
            candidates.append(str(env_path))

        keypair_env = os.getenv("KEYPAIR_PATH")
        if keypair_env:
            candidates.append(str(keypair_env))

        if not candidates:
            return None, None

        last_path: str | None = None
        for raw in candidates:
            if not raw:
                continue
            path_obj = Path(str(raw)).expanduser()
            if not path_obj.is_absolute():
                path_obj = ROOT / path_obj
            path_str = str(path_obj)
            last_path = path_str
            try:
                keypair = wallet.load_keypair(path_str)
            except FileNotFoundError:
                logger.warning("Keypair file not found at %s", path_str)
                continue
            except Exception as exc:
                logger.warning("Failed to load keypair from %s: %s", path_str, exc)
                continue
            return keypair, path_str

        return None, last_path

    def _get_rl_policy_confidence(self) -> Dict[str, float]:
        """Return latest RL policy confidence scores if available."""
        conf = dict(self.rl_policy)
        path = os.getenv("RL_POLICY_PATH", "rl_policy.json")
        if not os.path.isabs(path):
            path = str(ROOT / path)
        if os.path.exists(path):
            raw = None
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    raw = fh.read()
                data = loads(raw)
                if isinstance(data, dict):
                    conf.update({str(k): float(v) for k, v in data.items()})
            except Exception as exc:
                logger.warning(
                    "Failed to load RL policy from %s: %s; payload=%r",
                    path,
                    exc,
                    raw,
                )
        return conf

    async def evaluate_with_swarm(
        self, token: str, portfolio
    ) -> EvaluationContext:
        agents = self._select_agents()
        logger.info(
            "AgentManager: evaluating %s agents for token %s", len(agents), token
        )
        publish(
            "runtime.log",
            RuntimeLog(
                stage="evaluation",
                detail=f"agents:{token}:{','.join(a.name for a in agents)}",
            ),
        )
        regime = detect_regime(portfolio.price_history.get(token, []))
        weights = await self.coordinator.compute_weights(agents, regime=regime)
        logger.info("AgentManager: weights computed for %s", token)
        if weights:
            sorted_weights = sorted(weights.items(), key=lambda item: item[1], reverse=True)
            top_weights = ", ".join(
                f"{name}={weight:.3f}" for name, weight in sorted_weights[:5]
            )
            logger.debug(
                "AgentManager: top weights for %s -> %s%s",
                token,
                top_weights,
                " ..." if len(sorted_weights) > 5 else "",
            )
        else:
            logger.warning("AgentManager: no weights produced for %s", token)
        if self.selector:
            agents, weights = await self.selector.weight_agents(agents, weights)
            logger.info("AgentManager: selector adjusted weights for %s", token)
        if self.supervisor:
            sup_w = self.supervisor.predict_weights(
                [a.name for a in agents], token, portfolio
            )
            selected = []
            for ag in agents:
                w = sup_w.get(ag.name, 1.0)
                if w <= 0:
                    continue
                weights[ag.name] = weights.get(ag.name, 1.0) * float(w)
                selected.append(ag)
            agents = selected
        if self.rl_daemon is not None and getattr(self.rl_daemon, "hier_weights", None):
            pol_w = None
            try:
                pol_w = self.rl_daemon.hier_weights
                for ag in agents:
                    w = pol_w.get(ag.name)
                    if w is not None:
                        weights[ag.name] = weights.get(ag.name, 1.0) * float(w)
            except Exception as exc:
                logger.warning(
                    "Failed to merge RL daemon weights %r: %s; using existing weights",
                    pol_w,
                    exc,
                )
        rl_action = None
        if self.rl_daemon is not None:
            prices = portfolio.price_history.get(token, [])
            price = prices[-1] if prices else 0.0
            try:
                from .order_book_ws import snapshot

                depth, _imb, rate = snapshot(token)
            except Exception:
                depth = 0.0
                rate = 0.0
            try:
                rl_action = await self.rl_daemon.predict_action(
                    portfolio,
                    token,
                    price,
                    depth=depth,
                    tx_rate=rate,
                )
            except Exception:
                rl_action = None
            logger.debug(
                "AgentManager: RL daemon action for %s -> %s", token, rl_action
            )
        if self.use_attention_swarm and self.attention_swarm:
            rois, _ = await self.coordinator._roi_by_agent([a.name for a in agents])
            prices = portfolio.price_history.get(token, [])
            vol = (
                float(np.std(prices) / (np.mean(prices) or 1.0))
                if len(prices) > 1
                else 0.0
            )
            reg_val = 1.0 if regime == "bull" else -1.0 if regime == "bear" else 0.0
            self._attn_history.append(
                [rois.get(a.name, 0.0) for a in agents] + [reg_val, vol]
            )
            if len(self._attn_history) >= self.attention_swarm.seq_len:
                seq = self._attn_history[-self.attention_swarm.seq_len :]
                pred = self.attention_swarm.predict(seq)
                weights = {a.name: float(pred[i]) for i, a in enumerate(agents)}
                publish("weights_updated", WeightsUpdated(weights=dict(weights)))
        # Provide shared memory to the swarm when available so agents can
        # leverage recent summaries and we can log simulated outcomes.
        adv_mem = None
        try:
            adv = getattr(self.memory_agent, "memory", None) if self.memory_agent else None
            from .advanced_memory import AdvancedMemory as _Adv
            if isinstance(adv, _Adv):
                adv_mem = adv
        except Exception:
            adv_mem = None

        swarm = AgentSwarm(agents, memory=adv_mem, agent_timeout=self._agent_timeout)
        start = time.perf_counter()
        try:
            result = await swarm.propose(
                token,
                portfolio,
                weights=weights,
                rl_action=rl_action,
                regime=regime,
            )
        except asyncio.TimeoutError:
            logger.warning("Swarm evaluation timed out for %s", token)
            result = []
        latency = time.perf_counter() - start
        ctx = EvaluationContext(
            token=token,
            actions=result,
            swarm=swarm,
            agents=agents,
            weights=weights,
        )
        ctx.metadata = {"latency": latency, "regime": regime}
        logger.info("AgentManager: swarm produced %s actions for %s", len(result), token)
        if not result:
            logger.warning(
                "AgentManager: no actions produced for %s (regime=%s, latency=%.2fs)",
                token,
                regime,
                latency,
            )
        publish(
            "runtime.log",
            RuntimeLog(
                stage="evaluation",
                detail=f"swarm-done:{token}:{len(result)} actions latency={latency:.2f}s",
            ),
        )
        if swarm is not None:
            self._active_swarms[token] = swarm
        return ctx

    async def evaluate(self, token: str, portfolio) -> List[Dict[str, Any]]:
        ctx = await self.evaluate_with_swarm(token, portfolio)
        return ctx.actions

    def consume_swarm(
        self, token: str, default: AgentSwarm | None = None
    ) -> AgentSwarm | None:
        """Return and remove the cached swarm associated with ``token``."""

        return self._active_swarms.pop(token, default)

    def _install_threshold_profile(
        self, profile: Mapping[str, Mapping[str, float]]
    ) -> None:
        """Propagate decision thresholds to agents that can consume them."""

        for agent in self.agents:
            try:
                if hasattr(agent, "apply_threshold_profile"):
                    agent.apply_threshold_profile(copy.deepcopy(profile))
                elif hasattr(agent, "threshold_profile"):
                    setattr(agent, "threshold_profile", copy.deepcopy(profile))
            except Exception:
                continue

    def _select_agents(self) -> list[BaseAgent]:
        agents = list(self.agents)
        if not self._fast_mode:
            return agents
        include_env = os.getenv("FAST_AGENT_INCLUDE")
        exclude_env = os.getenv("FAST_AGENT_EXCLUDE")

        def _parse_list(raw: str | None) -> set[str]:
            if not raw:
                return set()
            return {item.strip().lower() for item in raw.split(",") if item.strip()}

        include_set = _parse_list(include_env)
        exclude_set = _parse_list(exclude_env)

        if not include_set:
            # Default fast-mode agent mix keeps the light, async-friendly set.
            include_set = {
                "simulation",
                "arbitrage",
                "momentum",
                "trend",
                "exit",
                "conviction",
            }

        if exclude_set:
            include_set.difference_update(exclude_set)
        if not include_set:
            include_set = {"simulation"}

        default_exclude = {
            "portfolio_optimizer",
            "portfolio_manager",
            "portfolio_agent",
            "smart_discovery",
            "supervisor",
            "flashloan",
            "mev",
            "crossdex",
            "artifact",
            "hierarchical",
            "rl_weight",
            "sac",
            "buy_hold",
            "mean_revert",
            "vanta",
            "inferna",
        }
        if exclude_set:
            default_exclude.update(exclude_set)

        def _matches(keys: set[str], name: str, cls: str) -> bool:
            return any(key and (key in name or key in cls) for key in keys)

        def keep(agent: BaseAgent) -> bool:
            name = agent.name.lower()
            cls = agent.__class__.__name__.lower()
            if include_set:
                if not _matches(include_set, name, cls) and "simulation" not in name and "simulation" not in cls:
                    return False
            if _matches(default_exclude, name, cls):
                return False
            return True

        filtered = [a for a in agents if keep(a)]
        if filtered:
            return filtered
        return agents

    async def execute(self, token: str, portfolio) -> List[Any]:
        ctx = await self.evaluate_with_swarm(token, portfolio)
        actions = list(ctx.actions)
        if not actions:
            logger.warning(
                "AgentManager: execute called for %s but no actions were produced", token
            )
        results = []
        if self.depth_service and token not in self._event_executors:
            execer = EventExecutor(
                token,
                priority_rpc=getattr(self.executor, "priority_rpc", None),
            )
            self._event_executors[token] = execer
            self.executor.add_executor(token, execer)
            self._event_tasks[token] = asyncio.create_task(execer.run())
        for action in actions:
            explain = ""
            ag_name = action.get("agent")
            if ag_name:
                agent = next((a for a in self.agents if a.name == ag_name), None)
                if agent is not None:
                    fn = getattr(agent, "explain_proposal", None)
                    if callable(fn):
                        try:
                            if inspect.iscoroutinefunction(fn):
                                explain = await fn(
                                    [action], token=token, portfolio=portfolio
                                )
                            else:
                                explain = fn([action], token=token, portfolio=portfolio)
                        except Exception:
                            explain = ""
            if explain:
                action.setdefault("context", explain)
            result = await self.executor.execute(action)
            if self.emotion_agent:
                emotion = self.emotion_agent.evaluate(action, result)
                action["emotion"] = emotion
            results.append(result)
            if self.memory_agent:
                await self.memory_agent.log(action)
            publish("action_executed", ActionExecuted(action=action, result=result))
        swarm = self.consume_swarm(token, ctx.swarm)
        if swarm is not None:
            try:
                swarm.record_results(results)
            except Exception:
                pass
        return results

    def _update_from_config(self, cfg: dict) -> None:
        weights = cfg.get("agent_weights")
        if isinstance(weights, str):
            try:
                import ast

                parsed = ast.literal_eval(weights)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                weights = parsed
            else:
                weights = {}
        if isinstance(weights, dict):
            for k, v in weights.items():
                try:
                    self.weights[str(k)] = float(v)
                except Exception:
                    continue
            self.coordinator.base_weights = self.weights

        ei = cfg.get("evolve_interval")
        if ei is not None:
            try:
                self.evolve_interval = int(ei)
            except Exception:
                pass
        mt = cfg.get("mutation_threshold")
        if mt is not None:
            try:
                self.mutation_threshold = float(mt)
            except Exception:
                pass

        rot_int = cfg.get("strategy_rotation_interval")
        if rot_int is not None:
            try:
                self.strategy_rotation_interval = int(rot_int)
            except Exception:
                pass

        w_paths = cfg.get("weight_config_paths")
        if w_paths is not None:
            if isinstance(w_paths, str):
                w_paths = [p.strip() for p in w_paths.split(",") if p.strip()]
            elif not isinstance(w_paths, list):
                w_paths = []
            self.weight_config_paths = list(w_paths)
            self.weight_configs = self._load_weight_configs(self.weight_config_paths)

        attn = cfg.get("use_attention_swarm")
        if attn is not None:
            self.use_attention_swarm = bool(attn)

        attn_path = cfg.get("attention_swarm_model")
        if attn_path is not None:
            try:
                attn_device = os.getenv(
                    "ATTENTION_SWARM_DEVICE", str(get_default_device())
                )
                self.attention_swarm = load_model(str(attn_path), device=attn_device)
            except Exception:
                self.attention_swarm = None

        reg_w = cfg.get("regime_weights")
        if reg_w is not None:
            if isinstance(reg_w, str):
                try:
                    import ast

                    parsed_r = ast.literal_eval(reg_w)
                    if isinstance(parsed_r, dict):
                        reg_w = parsed_r
                except Exception:
                    reg_w = None
            if isinstance(reg_w, dict):
                self.regime_weights = {
                    str(k): {str(sk): float(sv) for sk, sv in v.items()}
                    for k, v in reg_w.items()
                }
                self.coordinator.regime_weights = self.regime_weights

        rl_w = cfg.get("use_rl_weights")
        if rl_w is not None:
            self.use_rl_weights = bool(rl_w)
            if self.use_rl_weights and self.rl_weight_agent is None:
                self.rl_weight_agent = RLWeightAgent(
                    self.memory_agent,
                    weights_path=cfg.get("rl_weights_path", "rl_weights.json"),
                )
                self.agents.append(self.rl_weight_agent)
            elif not self.use_rl_weights and self.rl_weight_agent is not None:
                try:
                    self.agents.remove(self.rl_weight_agent)
                except ValueError:
                    pass
                self.rl_weight_agent = None

        rl_path = cfg.get("rl_weights_path")
        if rl_path is not None and self.rl_weight_agent is not None:
            self.rl_weight_agent.rl.weights_path = str(rl_path)

        sup_use = cfg.get("use_supervisor")
        sup_ckpt = cfg.get("supervisor_checkpoint")
        if sup_use is not None:
            if sup_use and self.supervisor is None:
                self.supervisor = SupervisorAgent(
                    checkpoint=str(sup_ckpt or "supervisor.json")
                )
            elif not sup_use and self.supervisor is not None:
                self.supervisor = None
        elif sup_ckpt is not None and self.supervisor is not None:
            self.supervisor.checkpoint = str(sup_ckpt)
            self.supervisor._load()

    async def update_weights(self) -> None:
        """Adjust agent weights based on historical trade ROI."""
        if not self.memory_agent:
            return

        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return
        trades = loader(limit=1000)
        if inspect.isawaitable(trades):
            trades = await trades
        summary: Dict[str, Dict[str, float]] = {}
        for t in trades:
            name = t.reason or ""
            info = summary.setdefault(name, {"buy": 0.0, "sell": 0.0})
            info[t.direction] += t.amount * t.price

        adv_mem = None
        if isinstance(self.memory_agent.memory, AdvancedMemory):
            adv_mem = self.memory_agent.memory

        rl_conf = self._get_rl_policy_confidence()

        for name, info in summary.items():
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent <= 0:
                continue
            roi = (revenue - spent) / spent
            success_rate = 0.0
            if adv_mem is not None:
                try:
                    success_rate = adv_mem.simulation_success_rate(name)
                except Exception:
                    success_rate = 0.0
            factor = 1.0
            if roi > 0:
                factor = 1.1
            elif roi < 0:
                factor = 0.9
            else:
                continue
            factor *= 1.0 + success_rate
            rl_factor = rl_conf.get(name)
            if rl_factor is not None:
                try:
                    factor *= float(rl_factor)
                except Exception:
                    pass
            self.weights[name] = self.weights.get(name, 1.0) * factor

        self.coordinator.base_weights = self.weights
        self.save_weights()
        publish("weights_updated", WeightsUpdated(weights=dict(self.weights)))

    def rotate_weight_configs(self) -> None:
        """Select the best preset weight config based on recent ROI."""
        if not self.memory_agent or not self.weight_configs:
            return
        analyzer = TradeAnalyzer(self.memory_agent.memory)
        rois = analyzer.roi_by_agent()
        best_name = None
        best_score = float("-inf")
        best_weights: Dict[str, float] | None = None
        for name, weights in self.weight_configs.items():
            score = 0.0
            for ag, w in weights.items():
                score += rois.get(ag, 0.0) * float(w)
            if score > best_score:
                best_score = score
                best_name = name
                best_weights = weights
        if best_weights is None:
            return
        changed = any(self.weights.get(k) != float(v) for k, v in best_weights.items())
        if changed:
            for k, v in best_weights.items():
                self.weights[str(k)] = float(v)
            self.coordinator.base_weights = self.weights
            self.save_weights()
            publish("weights_updated", WeightsUpdated(weights=dict(self.weights)))
        self.active_weight_config = best_name

    # ------------------------------------------------------------------
    #  Mutation helpers
    # ------------------------------------------------------------------
    async def _roi_by_agent(self, names: Iterable[str]) -> Dict[str, float]:
        if not self.memory_agent:
            return {n: 0.0 for n in names}
        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return {n: 0.0 for n in names}
        trades = loader(limit=1000)
        if inspect.isawaitable(trades):
            trades = await trades
        summary: Dict[str, Dict[str, float]] = {}
        for t in trades:
            reason = getattr(t, "reason", None)
            if reason not in names:
                continue
            direction = getattr(t, "direction", "")
            try:
                amount = float(getattr(t, "amount", 0.0))
                price = float(getattr(t, "price", 0.0))
            except Exception:
                continue
            dir_key = str(direction).lower()
            if dir_key not in {"buy", "sell"}:
                continue
            info = summary.setdefault(reason, {"buy": 0.0, "sell": 0.0})
            info[dir_key] += amount * price
        rois = {n: 0.0 for n in names}
        for name, info in summary.items():
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                rois[name] = (revenue - spent) / spent
        return rois

    def _active_names(self) -> list[str]:
        active = self.mutation_state.get("active", [])
        if active and isinstance(active[0], dict):
            return [str(a.get("name")) for a in active if isinstance(a, dict)]
        return [str(a) for a in active]

    def _restore_mutations(self) -> None:
        active = self.mutation_state.get("active", [])
        if not active or not isinstance(active[0], dict):
            return
        existing = {a.name for a in self.agents}
        for info in active:
            if not isinstance(info, dict):
                continue
            name = str(info.get("name"))
            if name in existing:
                continue
            base_cls = str(info.get("base"))
            params = (
                info.get("params", {})
                if isinstance(info.get("params", {}), dict)
                else {}
            )
            base_agent = next(
                (
                    a
                    for a in self.agents
                    if a.__class__.__name__ == base_cls or a.name == base_cls
                ),
                None,
            )
            if base_agent is None:
                try:
                    base_agent = load_agent(base_cls)
                except Exception:
                    try:
                        base_agent = load_agent(base_cls.lower())
                    except Exception:
                        base_agent = None
            if base_agent is None:
                continue
            cloned = mutation.clone_agent(base_agent, name=name, **params)
            self.agents.append(cloned)
            existing.add(name)

    def spawn_mutations(self, count: int = 1) -> List[BaseAgent]:
        base_agents = [
            a
            for a in self.agents
            if a.name not in self._active_names() and not isinstance(a, MemoryAgent)
        ]
        spawned: List[BaseAgent] = []
        if not base_agents:
            return spawned
        for _ in range(count):
            base = random.choice(base_agents)
            name = f"{base.name}_m{len(self._active_names()) + 1}"
            mutated = mutation.mutate_agent(base, name=name)
            self.agents.append(mutated)
            self.mutation_state.setdefault("active", []).append(
                {"name": mutated.name, "base": base.__class__.__name__, "params": {}}
            )
            spawned.append(mutated)
        return spawned

    async def prune_underperforming(self, threshold: float = 0.0) -> None:
        entries = list(self.mutation_state.get("active", []))
        if not entries:
            return
        name_map = {}
        names = []
        for e in entries:
            if isinstance(e, dict):
                name_map[e.get("name")] = e
                names.append(str(e.get("name")))
            else:
                name_map[e] = e
                names.append(str(e))
        rois = await self._roi_by_agent(names)
        keep_entries = []
        remaining_agents = []
        for agent in self.agents:
            if agent.name in names:
                if rois.get(agent.name, 0.0) >= threshold:
                    remaining_agents.append(agent)
                    keep_entries.append(name_map[agent.name])
            else:
                remaining_agents.append(agent)
        self.agents = remaining_agents
        self.mutation_state["active"] = keep_entries
        self.mutation_state.setdefault("roi", {}).update(rois)

    def save_mutation_state(self, path: str | os.PathLike | None = None) -> None:
        path = path or self.mutation_path
        if path:
            mutation.save_state(self.mutation_state, str(path))

    def _load_price_history(self) -> List[float]:
        data = load_sample_ticks(_TICKS_PATH)
        if data and isinstance(data[0], dict):
            return [float(d.get("price", 0.0)) for d in data]
        try:
            return [float(x) for x in data]
        except Exception:
            return []

    async def evolve(self, spawn_count: int = 1, threshold: float = 0.0) -> None:
        if self.population_rl is not None:
            best = await self.population_rl.evolve(agent.name for agent in self.agents)
            best_weights = best.get("weights", {}) if isinstance(best, dict) else {}
            if isinstance(best_weights, dict):
                self.weights.update(best_weights)
                self.coordinator.base_weights = self.weights
                self.save_weights()

        new_agents = self.spawn_mutations(spawn_count)
        prices = self._load_price_history()
        if prices:
            baseline = backtest_weighted(
                prices, self.weights, strategies=DEFAULT_STRATEGIES
            ).roi
            for agent in list(new_agents):
                test_weights = dict(self.weights)
                test_weights[agent.name] = 1.0
                roi = backtest_weighted(
                    prices, test_weights, strategies=DEFAULT_STRATEGIES
                ).roi
                if roi < baseline:
                    self.agents.remove(agent)
                    act = self.mutation_state.get("active", [])
                    for i, info in enumerate(list(act)):
                        if (
                            isinstance(info, dict) and info.get("name") == agent.name
                        ) or info == agent.name:
                            act.pop(i)
                            break
                else:
                    if self.memory_agent and isinstance(
                        self.memory_agent.memory, AdvancedMemory
                    ):
                        sim_id = self.memory_agent.memory.log_simulation(
                            "MUT",
                            expected_roi=roi,
                            success_prob=1.0 if roi > 0 else 0.0,
                        )
                        log_trade = getattr(self.memory_agent.memory, "log_trade", None)
                        if callable(log_trade):
                            async def _maybe_call(**kwargs: Any) -> None:
                                outcome = log_trade(**kwargs)
                                if inspect.isawaitable(outcome):
                                    await outcome

                            await _maybe_call(
                                token="MUT",
                                direction="buy",
                                amount=1.0,
                                price=1.0,
                                reason=agent.name,
                                simulation_id=sim_id,
                            )
                            await _maybe_call(
                                token="MUT",
                                direction="sell",
                                amount=1.0,
                                price=1.0 + roi,
                                reason=agent.name,
                                simulation_id=sim_id,
                            )

            keys = [name for name, _ in DEFAULT_STRATEGIES]
            try:
                opt = bayesian_optimize_weights(
                    prices, keys, DEFAULT_STRATEGIES, iterations=10
                )
                self.weights.update(opt)
                self.coordinator.base_weights = self.weights
            except Exception as exc:
                logger.exception("bayesian_optimize_weights failed", exc_info=exc)

        await self.prune_underperforming(threshold)
        if self.mutation_path:
            self.save_mutation_state()

    # ------------------------------------------------------------------
    #  Persistence helpers
    # ------------------------------------------------------------------
    def _load_weights(self, path: str | os.PathLike) -> Dict[str, float]:
        try:
            if str(path).endswith(".toml"):
                with open(path, "rb") as fh:
                    data = tomllib.load(fh)
            else:
                with open(path, "r", encoding="utf-8") as fh:
                    data = loads(fh.read())
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        return {str(k): float(v) for k, v in data.items()}

    def _load_weight_configs(self, paths: Iterable[str]) -> Dict[str, Dict[str, float]]:
        configs: Dict[str, Dict[str, float]] = {}
        for p in paths:
            w = self._load_weights(p)
            if w:
                configs[os.path.basename(p)] = w
        return configs

    def save_weights(self, path: str | os.PathLike | None = None) -> None:
        path = path or self.weights_path
        if not path:
            return
        if str(path).endswith(".toml"):
            lines = [f"{k} = {v}" for k, v in self.weights.items()]
            content = "\n".join(lines) + "\n"
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
        else:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(dumps(self.weights))

    # ------------------------------------------------------------------
    #  Convenience helpers
    # ------------------------------------------------------------------
    async def discover_tokens(
        self,
        *,
        offline: bool = False,
        token_file: str | None = None,
        method: str | None = None,
    ) -> List[str]:
        for agent in self.agents:
            if isinstance(agent, DiscoveryAgent):
                return await agent.discover_tokens(
                    offline=offline, token_file=token_file, method=method
                )
        disc = DiscoveryAgent()
        return await disc.discover_tokens(
            offline=offline, token_file=token_file, method=method
        )

    @classmethod
    def from_config(cls, cfg: dict) -> "AgentManager | None":
        names = cfg.get("agents", [])
        if isinstance(names, str):
            try:
                import ast

                parsed = ast.literal_eval(names)
                if isinstance(parsed, list):
                    names = parsed
                else:
                    names = [n.strip() for n in names.split(",") if n.strip()]
            except Exception:
                names = [n.strip() for n in names.split(",") if n.strip()]
        agents = []
        for name in names:
            try:
                agents.append(load_agent(name))
            except KeyError:
                continue
        weights = cfg.get("agent_weights") or {}
        if isinstance(weights, str):
            try:
                import ast

                parsed_w = ast.literal_eval(weights)
                if isinstance(parsed_w, dict):
                    weights = parsed_w
                else:
                    weights = {}
            except Exception:
                weights = {}
        weights_path = cfg.get("weights_path")
        memory_agent = next(
            (a for a in agents if isinstance(a, MemoryAgent)),
            None,
        )
        emotion_agent = next(
            (a for a in agents if isinstance(a, EmotionAgent)),
            None,
        )
        strategy_selection = bool(cfg.get("strategy_selection", False))
        vote_threshold = float(cfg.get("vote_threshold", 0.0) or 0.0)
        depth_service = bool(cfg.get("depth_service", False))
        priority_rpc = cfg.get("priority_rpc")
        if isinstance(priority_rpc, str):
            priority_rpc = [u.strip() for u in priority_rpc.split(",") if u.strip()]
        elif not isinstance(priority_rpc, list):
            priority_rpc = None
        regime_weights = cfg.get("regime_weights") or {}
        if isinstance(regime_weights, str):
            try:
                import ast

                parsed_r = ast.literal_eval(regime_weights)
                if isinstance(parsed_r, dict):
                    regime_weights = parsed_r
                else:
                    regime_weights = {}
            except Exception:
                regime_weights = {}
        decision_thresholds = cfg.get("decision_thresholds") or {}
        if isinstance(decision_thresholds, str):
            try:
                import ast

                parsed_dt = ast.literal_eval(decision_thresholds)
                if isinstance(parsed_dt, dict):
                    decision_thresholds = parsed_dt
                else:
                    decision_thresholds = {}
            except Exception:
                decision_thresholds = {}
        evolve_interval = int(cfg.get("evolve_interval", 1))
        mutation_threshold = float(cfg.get("mutation_threshold", 0.0))
        weight_config_paths = cfg.get("weight_config_paths") or []
        if isinstance(weight_config_paths, str):
            weight_config_paths = [
                p.strip() for p in weight_config_paths.split(",") if p.strip()
            ]
        strategy_rotation_interval = int(cfg.get("strategy_rotation_interval", 0))

        use_attention_swarm = bool(cfg.get("use_attention_swarm", False))
        attention_swarm_model = cfg.get("attention_swarm_model")

        use_rl_weights = bool(cfg.get("use_rl_weights", False))
        rl_weights_path = cfg.get("rl_weights_path")

        use_supervisor = bool(cfg.get("use_supervisor", False))
        supervisor_checkpoint = cfg.get("supervisor_checkpoint")

        hier_flag = cfg.get("hierarchical_rl")
        hier_model = cfg.get("hierarchical_model_path", "hier_policy.json")
        hierarchical_rl = None
        if hier_flag:
            try:
                hierarchical_rl = MultiAgentRL(controller_path=str(hier_model))
            except Exception:
                hierarchical_rl = None
                use_supervisor = True
                supervisor_checkpoint = supervisor_checkpoint or str(hier_model)

        jito_rpc_url = cfg.get("jito_rpc_url")
        jito_auth = cfg.get("jito_auth")
        jito_ws_url = cfg.get("jito_ws_url")
        jito_ws_auth = cfg.get("jito_ws_auth")
        if jito_rpc_url and os.getenv("JITO_RPC_URL") is None:
            os.environ["JITO_RPC_URL"] = str(jito_rpc_url)
        if jito_auth and os.getenv("JITO_AUTH") is None:
            os.environ["JITO_AUTH"] = str(jito_auth)
        if jito_ws_url and os.getenv("JITO_WS_URL") is None:
            os.environ["JITO_WS_URL"] = str(jito_ws_url)
        if jito_ws_auth and os.getenv("JITO_WS_AUTH") is None:
            os.environ["JITO_WS_AUTH"] = str(jito_ws_auth)

        keypair, keypair_path = cls._resolve_keypair_from_config(cfg)

        if not agents:
            return None
        cfg_obj = AgentManagerConfig(
            weights=weights,
            memory_agent=memory_agent,
            emotion_agent=emotion_agent,
            weights_path=weights_path,
            strategy_selection=strategy_selection,
            vote_threshold=vote_threshold,
            depth_service=depth_service,
            priority_rpc=priority_rpc,
            regime_weights=regime_weights,
            decision_thresholds=decision_thresholds,
            evolve_interval=evolve_interval,
            mutation_threshold=mutation_threshold,
            weight_config_paths=weight_config_paths,
            strategy_rotation_interval=strategy_rotation_interval,
            use_attention_swarm=use_attention_swarm,
            attention_model_path=attention_swarm_model,
            use_rl_weights=use_rl_weights,
            rl_weights_path=rl_weights_path,
            hierarchical_rl=hierarchical_rl,
            use_supervisor=use_supervisor,
            supervisor_checkpoint=supervisor_checkpoint,
            keypair=keypair,
            keypair_path=keypair_path,
        )
        return cls(agents, config=cfg_obj)

    @classmethod
    def from_default(cls) -> "AgentManager | None":
        """Instantiate with bundled default agent configuration."""
        default_path = (
            Path(__file__).resolve().parent.parent / "config" / "default.toml"
        )
        try:
            cfg = load_config(default_path)
        except Exception:
            cfg = {"agents": ["simulation"], "agent_weights": {"simulation": 1.0}}
        return cls.from_config(cfg)

    def close(self) -> None:
        for sub in getattr(self, "_subscriptions", []):
            try:
                sub.__exit__(None, None, None)
            except Exception:
                pass

        if getattr(self, "weights_path", None):
            try:
                self.save_weights()
            except Exception:
                pass

        if getattr(self, "mutation_path", None):
            try:
                self.save_mutation_state()
            except Exception:
                pass

        mem_agent = getattr(self, "memory_agent", None)
        if mem_agent is not None:
            close_fn = getattr(getattr(mem_agent, "memory", None), "close", None)
            if close_fn:
                try:
                    if inspect.iscoroutinefunction(close_fn):
                        from .util import run_coro

                        run_coro(close_fn())
                    else:
                        close_fn()
                except Exception:
                    pass
