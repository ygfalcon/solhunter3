from __future__ import annotations

import os

from typing import Any, Mapping


_TRUE = {"1", "true", "yes", "on", "enabled"}
_FALSE = {"0", "false", "no", "off", "disabled"}


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in _TRUE:
        return True
    if normalized in _FALSE:
        return False
    return None


def _lookup_config_flag(config: Mapping[str, Any] | None) -> bool | None:
    if not isinstance(config, Mapping):
        return None

    def _extract(mapping: Mapping[str, Any] | None, key: str) -> Any:
        if not isinstance(mapping, Mapping):
            return None
        return mapping.get(key)

    golden_cfg = _extract(config, "golden")
    if isinstance(golden_cfg, Mapping):
        direct = _parse_bool(str(golden_cfg.get("depth_enabled"))) if golden_cfg.get("depth_enabled") is not None else None
        if direct is not None:
            return direct
        depth_cfg = _extract(golden_cfg, "depth")
        if isinstance(depth_cfg, Mapping):
            enabled = depth_cfg.get("enabled")
            if isinstance(enabled, str):
                parsed = _parse_bool(enabled)
            else:
                parsed = bool(enabled) if isinstance(enabled, (bool, int)) else None
            if parsed is not None:
                return parsed
    legacy_cfg = _extract(config, "golden_pipeline")
    if isinstance(legacy_cfg, Mapping):
        enabled = legacy_cfg.get("depth_enabled")
        if enabled is not None:
            if isinstance(enabled, str):
                parsed = _parse_bool(enabled)
            else:
                parsed = bool(enabled) if isinstance(enabled, (bool, int)) else None
            if parsed is not None:
                return parsed
    return None


def resolve_depth_flag(config: Mapping[str, Any] | None = None) -> bool:
    env_override = _parse_bool(os.getenv("GOLDEN_DEPTH_ENABLED"))
    if env_override is not None:
        return env_override

    config_flag = _lookup_config_flag(config)
    if config_flag is not None:
        return config_flag

    # Depth extensions are part of the public Golden snapshot contract, so they must
    # remain enabled unless explicitly disabled through config or environment
    # overrides. Historically this default flipped off in live mode; keep the same
    # override hooks but default to ``True`` for every runtime mode.
    return True


def resolve_momentum_flag(config: Mapping[str, Any] | None = None) -> bool:
    env_override = _parse_bool(os.getenv("GOLDEN_MOMENTUM_ENABLED"))
    if env_override is not None:
        return env_override

    if isinstance(config, Mapping):
        golden_cfg = config.get("golden")
        if isinstance(golden_cfg, Mapping):
            momentum_cfg = golden_cfg.get("momentum")
            if isinstance(momentum_cfg, Mapping):
                flag = momentum_cfg.get("enabled")
                if isinstance(flag, str):
                    parsed = _parse_bool(flag)
                else:
                    parsed = bool(flag) if isinstance(flag, (bool, int)) else None
                if parsed is not None:
                    return bool(parsed)
            direct = golden_cfg.get("momentum_enabled")
            if isinstance(direct, str):
                parsed = _parse_bool(direct)
            else:
                parsed = bool(direct) if isinstance(direct, (bool, int)) else None
            if parsed is not None:
                return bool(parsed)
    return False


__all__ = ["resolve_depth_flag", "resolve_momentum_flag"]
