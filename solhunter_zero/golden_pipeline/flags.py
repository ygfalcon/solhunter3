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

    mode = (os.getenv("MODE") or os.getenv("SOLHUNTER_MODE") or "paper").strip().lower()
    if mode in {"live", "production", "prod"}:
        return False
    return True


__all__ = ["resolve_depth_flag"]
