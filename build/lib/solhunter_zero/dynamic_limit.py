import os
import logging

try:  # pragma: no cover - optional dependency
    import psutil
except Exception:  # pragma: no cover - psutil optional
    psutil = None  # type: ignore
    logging.getLogger(__name__).warning(
        "psutil not installed; memory-based concurrency limits disabled"
    )

# Module level parameters read once on import.
_KP: float = float(
    os.getenv("CONCURRENCY_SMOOTHING", os.getenv("CONCURRENCY_KP", "0.5")) or 0.5
)
_KI: float = float(os.getenv("CONCURRENCY_KI", "0.0") or 0.0)
_EWM_SMOOTHING: float = float(
    os.getenv("CONCURRENCY_EWM_SMOOTHING", "0.15") or 0.15
)
_ERR_INT: float = 0.0
_CPU_EMA: float = 0.0

# Share the current parameters via environment variables so child processes
# started after import see the same values.
os.environ["CONCURRENCY_SMOOTHING"] = str(_KP)
os.environ["CONCURRENCY_KI"] = str(_KI)
os.environ["CONCURRENCY_EWM_SMOOTHING"] = str(_EWM_SMOOTHING)


def refresh_params() -> None:
    """Reload concurrency parameters from environment variables."""
    global _KP, _KI, _EWM_SMOOTHING
    _KP = float(
        os.getenv("CONCURRENCY_SMOOTHING", os.getenv("CONCURRENCY_KP", str(_KP))) or _KP
    )
    _KI = float(os.getenv("CONCURRENCY_KI", str(_KI)) or _KI)
    _EWM_SMOOTHING = float(
        os.getenv("CONCURRENCY_EWM_SMOOTHING", str(_EWM_SMOOTHING)) or _EWM_SMOOTHING
    )
    os.environ["CONCURRENCY_SMOOTHING"] = str(_KP)
    os.environ["CONCURRENCY_KI"] = str(_KI)
    os.environ["CONCURRENCY_EWM_SMOOTHING"] = str(_EWM_SMOOTHING)


def _target_concurrency(
    cpu: float,
    base: int,
    low: float,
    high: float,
    *,
    smoothing: float | None = None,
) -> int:
    """Return desired concurrency for ``cpu`` usage.

    The function smooths CPU usage using an exponential moving average and
    adjusts the target based on current memory pressure.
    """

    global _CPU_EMA

    if smoothing is None:
        smoothing = _EWM_SMOOTHING

    if _CPU_EMA:
        _CPU_EMA = smoothing * cpu + (1.0 - smoothing) * _CPU_EMA
    else:
        _CPU_EMA = cpu

    if psutil is None:
        mem = 0.0
    else:
        try:
            mem = float(psutil.virtual_memory().percent)
        except Exception:
            mem = 0.0

    load = max(_CPU_EMA, mem)

    if load <= low:
        return base
    if load >= high:
        return 1
    frac = (load - low) / (high - low)
    return max(1, int(round(base * (1.0 - frac))))


def _step_limit(
    current: int,
    target: int,
    max_val: int,
    *,
    smoothing: float | None = None,
    ki: float | None = None,
) -> int:
    """Move ``current`` towards ``target`` using a PI controller."""

    global _ERR_INT

    if smoothing is None:
        smoothing = _KP
    if ki is None:
        ki = _KI

    err = target - current
    _ERR_INT += ki * err
    _ERR_INT = max(-max_val, min(max_val, _ERR_INT))
    new_val = current + smoothing * err + _ERR_INT
    new_val = int(round(new_val))
    if new_val > max_val:
        return max_val
    if new_val < 1:
        return 1
    return new_val
