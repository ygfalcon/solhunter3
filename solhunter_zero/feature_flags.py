"""Feature flag helpers for SolHunter."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Callable, Dict, Mapping

from .logging_utils import log_startup

__all__ = ["FeatureFlags", "get_feature_flags", "emit_feature_flag_metrics"]


_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default


@dataclass(frozen=True)
class FeatureFlags:
    """Capture the runtime feature toggles that coordinate the swarm."""

    mode: str = "paper"
    micro_mode: bool = False
    new_das_discovery: bool = True
    exit_features_on: bool = True
    rl_weights_disabled: bool = True
    das_enabled: bool = True
    mint_stream_enabled: bool = False
    mempool_stream_enabled: bool = False
    amm_watch_enabled: bool = False
    seeded_tokens_enabled: bool = False
    rl_shadow_mode: bool = False
    paper_trading: bool = True
    live_trading_disabled: bool = True

    @classmethod
    def from_env(cls) -> "FeatureFlags":
        """Create a :class:`FeatureFlags` snapshot from environment variables."""

        mode = os.getenv("MODE", "paper").strip().lower() or "paper"
        discovery_provider = os.getenv("ONCHAIN_DISCOVERY_PROVIDER", "").strip().lower()
        das_provider_enabled = discovery_provider in {"das", "helius"}
        das_flag = _get_bool("ONCHAIN_USE_DAS", True)
        das_discovery = _get_bool("NEW_DAS_DISCOVERY", True)
        mint_stream = _get_bool("MINT_STREAM_ENABLE", False)
        mempool_stream = _get_bool("MEMPOOL_STREAM_ENABLE", False)
        amm_watch = _get_bool("AMM_WATCH_ENABLE", False)
        seeded_enabled = _get_bool("SEED_PUBLISH_ENABLE", False)
        has_seed_tokens = bool(os.getenv("SEED_TOKENS", "").strip())
        rl_shadow = _get_bool("SHADOW_EXECUTOR_ONLY", mode == "shadow")
        paper_toggle = _get_bool("PAPER_TRADING", mode != "live")
        live_disabled = _get_bool("LIVE_TRADING_DISABLED", mode != "live")
        das_active = das_flag or das_provider_enabled or das_discovery
        return cls(
            mode=mode,
            micro_mode=_get_bool("MICRO_MODE", False),
            new_das_discovery=das_discovery,
            exit_features_on=_get_bool("EXIT_FEATURES_ON", True),
            rl_weights_disabled=_get_bool("RL_WEIGHTS_DISABLED", True),
            das_enabled=das_active,
            mint_stream_enabled=mint_stream,
            mempool_stream_enabled=mempool_stream,
            amm_watch_enabled=amm_watch,
            seeded_tokens_enabled=seeded_enabled and has_seed_tokens,
            rl_shadow_mode=rl_shadow,
            paper_trading=paper_toggle,
            live_trading_disabled=live_disabled,
        )

    # Mapping used by the acceptance checklist and metrics publishers.
    def as_metrics(self) -> Dict[str, float]:
        """Return numeric metrics for the feature flags (1.0/0.0 values)."""

        def _bool_metric(value: bool) -> float:
            return 1.0 if value else 0.0

        mode_value = {
            "paper": 0.0,
            "live": 1.0,
            "shadow": -0.5,
        }.get(self.mode, -1.0)
        return {
            "mode": mode_value,
            "micro_mode": _bool_metric(self.micro_mode),
            "new_das_discovery": _bool_metric(self.new_das_discovery),
            "exit_features_on": _bool_metric(self.exit_features_on),
            "rl_weights_disabled": _bool_metric(self.rl_weights_disabled),
            "das_enabled": _bool_metric(self.das_enabled),
            "mint_stream_enabled": _bool_metric(self.mint_stream_enabled),
            "mempool_stream_enabled": _bool_metric(self.mempool_stream_enabled),
            "amm_watch_enabled": _bool_metric(self.amm_watch_enabled),
            "seeded_tokens_enabled": _bool_metric(self.seeded_tokens_enabled),
            "rl_shadow_mode": _bool_metric(self.rl_shadow_mode),
            "paper_trading": _bool_metric(self.paper_trading),
            "live_trading_disabled": _bool_metric(self.live_trading_disabled),
        }

    def describe(self) -> str:
        """Generate a human readable description for startup logging."""

        status = "micro" if self.micro_mode else self.mode
        toggles = {
            "DAS": self.das_enabled,
            "MintStream": self.mint_stream_enabled,
            "Mempool": self.mempool_stream_enabled,
            "AMMWatch": self.amm_watch_enabled,
            "Seeded": self.seeded_tokens_enabled,
            "RLShadow": self.rl_shadow_mode,
            "PaperTrading": self.paper_trading,
            "LiveDisabled": self.live_trading_disabled,
        }
        toggle_desc = ", ".join(
            f"{name}={'on' if enabled else 'off'}" for name, enabled in toggles.items()
        )
        return (
            f"MODE={self.mode} (active profile: {status}); "
            f"NEW_DAS_DISCOVERY={'on' if self.new_das_discovery else 'off'}; "
            f"EXIT_FEATURES_ON={'on' if self.exit_features_on else 'off'}; "
            f"RL_WEIGHTS_DISABLED={'on' if self.rl_weights_disabled else 'off'}; "
            f"{toggle_desc}"
        )

    def for_ui(self) -> Mapping[str, bool | str]:
        """Return a mapping suitable for surfacing the flags in the UI."""

        return {
            "mode": self.mode,
            "micro_mode": self.micro_mode,
            "new_das_discovery": self.new_das_discovery,
            "exit_features_on": self.exit_features_on,
            "rl_weights_disabled": self.rl_weights_disabled,
            "das_enabled": self.das_enabled,
            "mint_stream_enabled": self.mint_stream_enabled,
            "mempool_stream_enabled": self.mempool_stream_enabled,
            "amm_watch_enabled": self.amm_watch_enabled,
            "seeded_tokens_enabled": self.seeded_tokens_enabled,
            "rl_shadow_mode": self.rl_shadow_mode,
            "paper_trading": self.paper_trading,
            "live_trading_disabled": self.live_trading_disabled,
        }


def get_feature_flags() -> FeatureFlags:
    """Convenience accessor mirroring :meth:`FeatureFlags.from_env`."""

    return FeatureFlags.from_env()


def emit_feature_flag_metrics(publish: Callable[[str, float], None]) -> FeatureFlags:
    """Publish the feature flag metrics and return the snapshot used."""

    flags = FeatureFlags.from_env()
    for name, value in flags.as_metrics().items():
        publish(f"feature_flag_{name}", value)
    log_startup(f"Feature flags: {flags.describe()}")
    return flags
