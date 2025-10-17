from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, TypedDict, Literal
import inspect
import math
import re


_THRESHOLD_KEYS = ("threshold", "limit")
_BOUND_NOISE = {"min", "max", "minimum", "maximum", "lower", "upper", "floor", "ceiling"}
_MIN_KEYS = ("min_", "minimum_", "lower_", "floor_")
_MAX_KEYS = ("max_", "maximum_", "upper_", "ceiling_")

_WORD = re.compile(r"[A-Za-z0-9]+")


class RequirementEntry(TypedDict, total=False):
    parameter: str
    kind: Literal["min", "max", "threshold"]
    target: float
    metric: Optional[str]
    current: Optional[float]
    delta: float
    met: bool


def _is_threshold_attr(name: str) -> bool:
    lower = name.lower()
    words = set(_WORD.findall(lower))
    if words & set(_THRESHOLD_KEYS):
        return True
    if lower.startswith(_MIN_KEYS) or lower.endswith("_min"):
        return True
    if lower.startswith(_MAX_KEYS) or lower.endswith("_max"):
        return True
    return False


def _safe_numeric_members(obj: Any) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    try:
        for name, val in inspect.getmembers_static(obj):
            if not name.startswith("_"):
                fields[name] = val
    except Exception:
        try:
            fields.update({k: v for k, v in vars(obj).items() if not k.startswith("_")})
        except Exception:
            pass
    return fields


def _extract_numeric_fields(agent: Any) -> Dict[str, float]:
    fields: Dict[str, float] = {}
    for attr, value in _safe_numeric_members(agent).items():
        if not _is_threshold_attr(attr):
            continue
        if callable(value):
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric_value):
            fields[attr] = numeric_value
    return fields


def _normalise_name(name: str) -> str:
    tokens = re.split(r"[_\-\s]+", name.lower())
    filtered = [tok for tok in tokens if tok and tok not in _BOUND_NOISE and tok not in _THRESHOLD_KEYS]
    return "_".join(filtered) or name.lower()


def _normalize_metric_keys(metrics: Mapping[str, Any]) -> Dict[str, str]:
    return {_normalise_name(name): name for name in metrics.keys()}


def _match_metric(field: str, metrics: Mapping[str, Any], norm_map: Mapping[str, str]) -> Optional[str]:
    if not metrics:
        return None
    field_key = _normalise_name(field) or "roi"

    if field_key in norm_map:
        return norm_map[field_key]

    for norm_name, original in norm_map.items():
        haystack = f"_{norm_name}_"
        needle = f"_{field_key}_"
        if needle in haystack or norm_name.startswith(field_key + "_") or norm_name.endswith("_" + field_key):
            return original

    # fallbacks for common metric types
    if "roi" in field.lower():
        for key in ("avg_roi", "max_roi", "min_roi", "roi"):
            if key in metrics:
                return key
    if any(token in field.lower() for token in ("success", "confidence", "prob")):
        for key in ("avg_success", "success", "success_prob", "confidence"):
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
    if "price" in field.lower():
        for key in ("avg_price", "price"):
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


def collect_agent_requirements(agent: Any, metrics: Mapping[str, Any] | None = None) -> List[RequirementEntry]:
    """Return a structured list of numeric requirements for ``agent``."""

    metrics = metrics or {}
    fields = _extract_numeric_fields(agent)
    norm_map = _normalize_metric_keys(metrics) if metrics else {}
    requirements: List[RequirementEntry] = []
    for field, value in fields.items():
        kind = _classify(field)
        metric_key = _match_metric(field, metrics, norm_map)
        current = None
        if metric_key is not None:
            metric_value = metrics.get(metric_key)
            try:
                metric_float = float(metric_value)
            except (TypeError, ValueError):
                metric_float = None
            else:
                if not math.isfinite(metric_float):
                    metric_float = None
            if metric_float is not None:
                current = metric_float
        entry: RequirementEntry = {
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
