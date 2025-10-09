from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional
import math
import re


_THRESHOLD_KEYS = ("threshold", "limit", "target")
_MIN_KEYS = ("min_", "minimum", "lower", "floor")
_MAX_KEYS = ("max_", "maximum", "upper", "ceiling")


def _is_threshold_attr(name: str) -> bool:
    lower = name.lower()
    if any(key in lower for key in _THRESHOLD_KEYS):
        return True
    if lower.startswith(_MIN_KEYS) or lower.startswith(_MAX_KEYS):
        return True
    if lower.endswith("_threshold") or lower.endswith("_limit"):
        return True
    return False


def _extract_numeric_fields(agent: Any) -> Dict[str, float]:
    fields: Dict[str, float] = {}
    for attr in dir(agent):
        if attr.startswith("_"):
            continue
        if not _is_threshold_attr(attr):
            continue
        try:
            value = getattr(agent, attr)
        except Exception:
            continue
        if isinstance(value, (int, float)) and math.isfinite(value):
            fields[attr] = float(value)
    return fields


def _normalise_name(name: str) -> str:
    tokens = re.split(r"[_\-\s]+", name.lower())
    filtered = [tok for tok in tokens if tok and tok not in {"threshold", "limit", "min", "max", "minimum", "maximum"}]
    return "_".join(filtered) or name.lower()


def _match_metric(field: str, metrics: Mapping[str, Any]) -> Optional[str]:
    if not metrics:
        return None
    field_key = _normalise_name(field)
    if not field_key or field_key == field.lower():
        field_key = field_key.replace("threshold", "").strip("_")
    if not field_key:
        field_key = "roi"
    for metric_name in metrics.keys():
        metric_key = metric_name.lower()
        if field_key and field_key in metric_key:
            return metric_name
    # fallbacks for common metric types
    if "roi" in field.lower():
        for key in ("avg_roi", "max_roi", "min_roi"):
            if key in metrics:
                return key
    if "success" in field.lower() or "confidence" in field.lower():
        for key in ("avg_success", "success", "success_prob"):
            if key in metrics:
                return key
    if "volume" in field.lower():
        for key in ("volume", "avg_volume"):
            if key in metrics:
                return key
    if "liquidity" in field.lower():
        for key in ("liquidity", "avg_liquidity"):
            if key in metrics:
                return key
    return None


def _classify(field: str) -> str:
    lower = field.lower()
    if lower.startswith(_MIN_KEYS) or lower.endswith("_min"):
        return "min"
    if lower.startswith(_MAX_KEYS) or lower.endswith("_max"):
        return "max"
    return "threshold"


def collect_agent_requirements(agent: Any, metrics: Mapping[str, Any] | None = None) -> List[Dict[str, Any]]:
    """Return a structured list of numeric requirements for ``agent``."""

    metrics = metrics or {}
    fields = _extract_numeric_fields(agent)
    requirements: List[Dict[str, Any]] = []
    for field, value in fields.items():
        kind = _classify(field)
        metric_key = _match_metric(field, metrics)
        current = None
        if metric_key is not None:
            metric_value = metrics.get(metric_key)
            try:
                current = float(metric_value)
            except (TypeError, ValueError):
                current = None
        entry: Dict[str, Any] = {
            "parameter": field,
            "kind": kind,
            "target": value,
            "metric": metric_key,
            "current": current,
        }
        if current is not None:
            if kind == "max":
                entry["delta"] = value - current
                entry["met"] = current <= value
            else:  # threshold/min behave the same
                entry["delta"] = current - value
                entry["met"] = current >= value
        requirements.append(entry)
    return requirements


__all__ = ["collect_agent_requirements"]
