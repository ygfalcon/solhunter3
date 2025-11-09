"""Shared RL daemon health gate helpers."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from collections.abc import Mapping
from typing import Any

from .feature_flags import FeatureFlags, get_feature_flags
from .health_runtime import http_ok, resolve_rl_health_url, wait_for
from .util import parse_bool_env

log = logging.getLogger(__name__)


@dataclass(slots=True)
class RLHealthGateResult:
    """Outcome of the RL daemon health gate."""

    url: str | None = None
    status: str | None = None
    skip_reason: str | None = None

    @property
    def skipped(self) -> bool:
        return self.skip_reason is not None and not self.url


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return max(1, default)
    try:
        value = int(raw)
    except Exception:
        log.warning("Invalid %s=%r; using default %d", name, raw, default)
        return max(1, default)
    return max(1, value)


def _parse_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return max(0.1, default)
    try:
        value = float(raw)
    except Exception:
        log.warning("Invalid %s=%r; using default %.2f", name, raw, default)
        return max(0.1, default)
    return max(0.1, value)


def _rl_health_gate_skip_reason(
    flags: FeatureFlags | None,
    config: Mapping[str, Any] | None,
) -> str | None:
    if flags is not None:
        mode = (flags.mode or "").lower()
        if mode != "live":
            return f"MODE={mode}" if mode else "mode unset"
        if getattr(flags, "rl_weights_disabled", False):
            return "RL_WEIGHTS_DISABLED=1"
    if isinstance(config, Mapping):
        use_rl = config.get("use_rl_weights")
        if use_rl is not None and not bool(use_rl):
            return "config.use_rl_weights disabled"
    return None


def rl_health_gate(
    *,
    flags: FeatureFlags | None = None,
    config: Mapping[str, Any] | None = None,
) -> RLHealthGateResult:
    """Block startup until the external RL daemon reports healthy."""

    if parse_bool_env("RL_HEALTH_BYPASS", False):
        reason = "RL_HEALTH_BYPASS=1"
        log.info("Bypassing RL health gate: %s", reason)
        return RLHealthGateResult(skip_reason=reason)

    if flags is None:
        try:
            flags = get_feature_flags()
        except Exception:  # pragma: no cover - defensive
            flags = None

    skip_reason = _rl_health_gate_skip_reason(flags, config)
    if skip_reason is not None:
        log.info("Skipping RL health gate: %s", skip_reason)
        return RLHealthGateResult(skip_reason=skip_reason)

    try:
        url = resolve_rl_health_url(require_health_file=True)
    except Exception as exc:
        raise RuntimeError(f"RL daemon gate failed: {exc}") from exc

    os.environ["RL_HEALTH_URL"] = url

    retries = _parse_positive_int_env("RL_HEALTH_RETRIES", 30)
    interval = _parse_positive_float_env("RL_HEALTH_INTERVAL", 1.0)
    log.info(
        "Waiting for RL daemon health endpoint %s (retries=%s, interval=%.2fs)",
        url,
        retries,
        interval,
    )

    ok, msg = wait_for(lambda: http_ok(url), retries=retries, sleep=interval)
    if not ok:
        raise RuntimeError(f"RL daemon gate failed: {msg}")

    log.info("RL daemon health confirmed: %s — %s", url, msg)
    return RLHealthGateResult(url=url, status=str(msg) if msg else None)
