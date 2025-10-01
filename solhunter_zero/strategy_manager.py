import importlib
import asyncio
import os
import contextlib
import logging
import pkgutil
from typing import Iterable, Any, List, Dict


logger = logging.getLogger(__name__)


class StrategyManager:
    """Load and execute multiple trading strategy modules."""

    DEFAULT_STRATEGIES = ["solhunter_zero.sniper", "solhunter_zero.arbitrage"]

    def __init__(
        self,
        strategies: Iterable[str] | None = None,
        *,
        env_var: str = "STRATEGIES",
        weights: Dict[str, float] | None = None,
        weights_env_var: str = "STRATEGY_WEIGHTS",
    ) -> None:
        if strategies is None:
            env = os.getenv(env_var)
            if env:
                strategies = [s.strip() for s in env.split(",") if s.strip()]
        if not strategies:
            strategies = self.DEFAULT_STRATEGIES

        if weights is None:
            env_w = os.getenv(weights_env_var)
            if env_w:
                weights = {}
                for item in env_w.split(","):
                    if "=" in item:
                        name, value = item.split("=", 1)
                        with contextlib.suppress(ValueError):
                            weights[name.strip()] = float(value)
        self._weights: Dict[str, float] = weights or {}

        self._modules: list[tuple[Any, str]] = []
        self._missing: list[str] = []
        for name in strategies:
            try:
                mod = importlib.import_module(name)
            except Exception as exc:  # pragma: no cover - optional strategies
                logger.warning("Failed to import strategy %s: %s", name, exc)
                self._missing.append(name)
                continue
            if hasattr(mod, "evaluate"):
                self._modules.append((mod, name))
            else:
                logger.warning(
                    "Strategy %s imported but has no evaluate attribute", name
                )

    def list_missing(self) -> List[str]:
        """Return strategies that failed to import."""
        return list(self._missing)

    async def evaluate(
        self,
        token: str,
        portfolio: Any,
        *,
        weights: Dict[str, float] | None = None,
        timeouts: Dict[str, float] | None = None,
    ) -> List[Dict[str, Any]]:
        """Run all strategies on ``token`` and return combined weighted actions.

        Parameters
        ----------
        weights:
            Optional mapping of module name to weight applied to its returned
            amounts. Defaults to ``1.0`` for all modules.
        timeouts:
            Optional mapping of module name to maximum time in seconds the
            strategy is allowed to run. Results from strategies exceeding the
            timeout are discarded.
        """

        weights_map = dict(self._weights)
        if weights:
            weights_map.update(weights)

        async def run_module(mod: Any, name: str) -> tuple[float, Any] | None:
            func = getattr(mod, "evaluate", None)
            if func is None:
                return None
            weight = float(weights_map.get(name, 1.0))
            timeout = None
            if timeouts and name in timeouts:
                timeout = float(timeouts[name])

            if asyncio.iscoroutinefunction(func):
                coro = func(token, portfolio)
            else:
                coro = asyncio.to_thread(func, token, portfolio)

            try:
                if timeout is not None:
                    res = await asyncio.wait_for(coro, timeout)
                else:
                    res = await coro
            except asyncio.TimeoutError:
                return None

            return weight, res

        tasks = [asyncio.create_task(run_module(mod, name)) for mod, name in self._modules]
        raw_results = await asyncio.gather(*tasks)

        actions: List[Dict[str, Any]] = []
        for item in raw_results:
            if not item:
                continue
            weight, res = item
            if not res:
                continue
            if isinstance(res, list):
                for r in res:
                    r = dict(r)
                    r["amount"] = float(r.get("amount", 0)) * weight
                    actions.append(r)
            else:
                r = dict(res)
                r["amount"] = float(r.get("amount", 0)) * weight
                actions.append(r)

        return self._merge_actions(actions)

    @staticmethod
    def _merge_actions(actions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Combine actions on the same token and side by summing amounts."""
        merged: dict[tuple[str, str], Dict[str, Any]] = {}
        for action in actions:
            token = action.get("token")
            side = action.get("side")
            if not token or not side:
                continue
            amt = float(action.get("amount", 0))
            price = float(action.get("price", 0))
            key = (token, side)
            m = merged.setdefault(key, {"token": token, "side": side, "amount": 0.0, "price": 0.0})
            old_amt = m["amount"]
            if old_amt + amt > 0:
                m["price"] = (m["price"] * old_amt + price * amt) / (old_amt + amt)
            m["amount"] += amt
        return list(merged.values())


def discover_strategy_modules() -> List[str]:
    """Return all non-private strategy modules in ``solhunter_zero.agents``.

    The function scans the :mod:`solhunter_zero.agents` package for modules
    that do not start with an underscore and returns their fully-qualified
    import paths.  Packages are ignored so only direct strategy modules are
    included.
    """

    import solhunter_zero.agents as agents_pkg

    modules = [
        f"{agents_pkg.__name__}.{name}"
        for _, name, ispkg in pkgutil.iter_modules(agents_pkg.__path__)
        if not ispkg and not name.startswith("_")
    ]
    return modules

