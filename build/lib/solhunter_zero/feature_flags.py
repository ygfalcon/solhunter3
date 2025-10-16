"""Feature flag helpers for SolHunter."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Callable, Dict

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

    @classmethod
    def from_env(cls) -> "FeatureFlags":
        """Create a :class:`FeatureFlags` snapshot from environment variables."""

        mode = os.getenv("MODE", "paper").strip().lower() or "paper"
        return cls(
            mode=mode,
            micro_mode=_get_bool("MICRO_MODE", False),
            new_das_discovery=_get_bool("NEW_DAS_DISCOVERY", True),
            exit_features_on=_get_bool("EXIT_FEATURES_ON", True),
            rl_weights_disabled=_get_bool("RL_WEIGHTS_DISABLED", True),
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
        }

    def describe(self) -> str:
        """Generate a human readable description for startup logging."""

        status = "micro" if self.micro_mode else self.mode
        return (
            f"MODE={self.mode} (active profile: {status}); "
            f"NEW_DAS_DISCOVERY={'on' if self.new_das_discovery else 'off'}; "
            f"EXIT_FEATURES_ON={'on' if self.exit_features_on else 'off'}; "
            f"RL_WEIGHTS_DISABLED={'on' if self.rl_weights_disabled else 'off'}"
        )


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
