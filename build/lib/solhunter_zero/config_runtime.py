from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Mapping
import copy
import logging


@dataclass
class Config:
    """Runtime configuration values populated from environment or file settings."""

    arbitrage_threshold: float = 0.0
    arbitrage_amount: float = 0.0
    recent_trade_window: float = 0.0
    solana_rpc_url: Optional[str] = None
    risk_tolerance: float = 0.1
    max_allocation: float = 0.2
    max_risk_per_token: float = 0.1
    risk_multiplier: float = 1.0
    min_portfolio_value: float = 20.0
    var_confidence: float = 0.95
    var_window: int = 30
    var_threshold: float = 0.0
    decision_thresholds: dict[str, dict[str, float]] | None = None

    _logger = logging.getLogger(__name__)

    @classmethod
    def from_env(cls, cfg: dict | None = None) -> "Config":
        """Create a Config instance using environment variables and optional dict."""
        cfg = cfg or {}
        env = os.getenv
        thresholds = cfg.get("decision_thresholds")
        if thresholds and not isinstance(thresholds, Mapping):
            thresholds = {}
        decision_profile: dict[str, dict[str, float]] | None = None
        try:
            from .decision import (
                set_global_threshold_profile,
                get_global_threshold_profile,
                get_baseline_thresholds,
            )

            set_global_threshold_profile(thresholds)
            active_profile = get_global_threshold_profile()
            baseline = get_baseline_thresholds()
            regimes = {
                scope: values
                for scope, values in active_profile.items()
                if scope != "default"
            }
            cls._logger.info(
                "Decision thresholds baseline=%s regimes=%s",
                baseline,
                regimes,
            )
            decision_profile = active_profile or None
        except Exception as exc:  # pragma: no cover - defensive fallback
            cls._logger.warning("Failed to install decision thresholds: %s", exc)

        return cls(
            arbitrage_threshold=float(
                env("ARBITRAGE_THRESHOLD", cfg.get("arbitrage_threshold", 0)) or 0
            ),
            arbitrage_amount=float(
                env("ARBITRAGE_AMOUNT", cfg.get("arbitrage_amount", 0)) or 0
            ),
            recent_trade_window=float(
                env("RECENT_TRADE_WINDOW", cfg.get("recent_trade_window", 0)) or 0
            ),
            solana_rpc_url=env("SOLANA_RPC_URL") or cfg.get("solana_rpc_url"),
            risk_tolerance=float(
                env("RISK_TOLERANCE", cfg.get("risk_tolerance", 0.1))
            ),
            max_allocation=float(
                env("MAX_ALLOCATION", cfg.get("max_allocation", 0.2))
            ),
            max_risk_per_token=float(
                env("MAX_RISK_PER_TOKEN", cfg.get("max_risk_per_token", 0.1))
            ),
            risk_multiplier=float(
                env("RISK_MULTIPLIER", cfg.get("risk_multiplier", 1.0))
            ),
            min_portfolio_value=float(
                env("MIN_PORTFOLIO_VALUE", cfg.get("min_portfolio_value", 20))
            ),
            var_confidence=float(
                env("VAR_CONFIDENCE", cfg.get("var_confidence", 0.95))
            ),
            var_window=int(env("VAR_WINDOW", cfg.get("var_window", 30))),
            var_threshold=float(
                env("VAR_THRESHOLD", cfg.get("var_threshold", 0.0))
            ),
            decision_thresholds=copy.deepcopy(decision_profile) if decision_profile else None,
        )
