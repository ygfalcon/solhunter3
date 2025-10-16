from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import logging
import os
import math
try:  # pragma: no cover - optional dependency
    import numpy as _np  # type: ignore
    if not hasattr(_np, "sort"):
        raise ImportError
    np = _np  # type: ignore
except Exception:  # pragma: no cover - numpy not available or incomplete
    np = None  # type: ignore

if np is None:
    class _Matrix(list):  # pragma: no cover - simple matrix fallback
        @property
        def shape(self) -> tuple[int, int]:
            if not self:
                return (0, 0)
            return (len(self), len(self[0]))

        def tolist(self) -> list[list[float]]:
            return [row[:] for row in self]

try:  # pragma: no cover - optional dependency
    from numba import njit as _numba_njit
except Exception:  # pragma: no cover - numba not available
    def njit(*args, **kwargs):  # type: ignore
        if args and callable(args[0]):
            return args[0]

        def wrapper(func):
            return func

        return wrapper
else:  # pragma: no cover - numba available
    def njit(*args, **kwargs):  # type: ignore
        def decorator(func):
            try:
                return _numba_njit(*args, **kwargs)(func)
            except Exception:
                return func

        if args and callable(args[0]):
            return decorator(args[0])

        return decorator

from .memory import Memory

logger = logging.getLogger(__name__)


def compute_live_covariance(prices: Mapping[str, Sequence[float]]) -> np.ndarray:
    """Return covariance matrix for ``prices`` recorded in real time."""

    return covariance_matrix(prices)


def compute_live_correlation(prices: Mapping[str, Sequence[float]]) -> np.ndarray:
    """Return correlation matrix for ``prices`` recorded in real time."""

    return correlation_matrix(prices)


def hedge_ratio(a: Sequence[float], b: Sequence[float]) -> float:
    """Return hedge ratio between two price series using returns."""

    if len(a) < 2 or len(b) < 2:
        return 0.0
    n = min(len(a), len(b))
    if np is None:
        arr_a = [float(x) for x in a[-n:]]
        arr_b = [float(x) for x in b[-n:]]
        ret_a = [arr_a[i + 1] / arr_a[i] - 1 for i in range(n - 1)]
        ret_b = [arr_b[i + 1] / arr_b[i] - 1 for i in range(n - 1)]
        mean_b = sum(ret_b) / len(ret_b)
        var_b = sum((x - mean_b) ** 2 for x in ret_b) / len(ret_b)
        if var_b == 0:
            return 0.0
        mean_a = sum(ret_a) / len(ret_a)
        cov = sum((ra - mean_a) * (rb - mean_b) for ra, rb in zip(ret_a, ret_b)) / len(ret_a)
        return cov / var_b
    arr_a = np.asarray(a[-n:], dtype=float)
    arr_b = np.asarray(b[-n:], dtype=float)
    ret_a = arr_a[1:] / arr_a[:-1] - 1
    ret_b = arr_b[1:] / arr_b[:-1] - 1
    var_b = np.var(ret_b)
    if var_b == 0:
        return 0.0
    cov = float(np.cov(ret_a, ret_b, ddof=0)[0, 1])
    return cov / var_b


def leverage_scaling(current: float, target: float) -> float:
    """Return scaling factor to adjust from ``current`` to ``target`` leverage."""

    if current <= 0:
        return 1.0
    if target <= 0:
        return 0.0
    return target / current


@njit
def value_at_risk(
    prices: Sequence[float], confidence: float = 0.95, memory: Memory | None = None
) -> float:
    """Return historical Value-at-Risk of ``prices``.

    ``prices`` should be ordered oldest to newest.  The VaR is returned as a
    positive fraction representing the maximum expected loss at the given
    confidence level.
    """

    if len(prices) < 2:
        var = 0.0
    else:
        returns = [prices[i + 1] / prices[i] - 1 for i in range(len(prices) - 1)]
        returns.sort()
        idx = int((1 - confidence) * len(returns))
        idx = max(0, min(idx, len(returns) - 1))
        var = -returns[idx]
        if var < 0:
            var = 0.0

    if memory is not None:
        try:  # pragma: no cover - logging failures are non-critical
            memory.log_var(var)
        except Exception as exc:  # pragma: no cover - log failure
            logger.exception("Failed to log VaR", exc_info=exc)

    return var


def recent_value_at_risk(
    prices: Sequence[float],
    *,
    window: int = 30,
    confidence: float = 0.95,
    memory: Memory | None = None,
) -> float:
    """Return VaR using the most recent ``window`` prices."""

    if len(prices) > window:
        slice_prices = prices[-window:]
    else:
        slice_prices = prices
    return value_at_risk(slice_prices, confidence, memory=memory)


@njit
def conditional_value_at_risk_prices(
    prices: Sequence[float], confidence: float = 0.95
) -> float:
    """Return CVaR computed from ``prices`` ordered oldest to newest."""

    if len(prices) < 2:
        return 0.0
    returns = [prices[i + 1] / prices[i] - 1 for i in range(len(prices) - 1)]
    return conditional_value_at_risk(returns, confidence)


def recent_conditional_value_at_risk(
    prices: Sequence[float],
    *,
    window: int = 30,
    confidence: float = 0.95,
) -> float:
    """Return CVaR using the most recent ``window`` prices."""

    if len(prices) > window:
        slice_prices = prices[-window:]
    else:
        slice_prices = prices
    return conditional_value_at_risk_prices(slice_prices, confidence)


@njit
def conditional_value_at_risk(
    returns: Sequence[float], confidence: float = 0.95
) -> float:
    """Return Conditional Value-at-Risk (expected shortfall) of ``returns``."""

    if len(returns) == 0:
        return 0.0
    if np is None:
        arr = sorted(float(x) for x in returns)
        cutoff = int((1 - confidence) * len(arr))
        cutoff = max(1, cutoff)
        tail = arr[:cutoff]
        cvar = -sum(tail) / len(tail)
        return max(cvar, 0.0)
    arr = np.sort(np.asarray(returns, dtype=float))
    cutoff = int((1 - confidence) * len(arr))
    cutoff = max(1, cutoff)
    tail = arr[:cutoff]
    cvar = -float(tail.mean())
    return max(cvar, 0.0)


@njit
def entropic_value_at_risk(
    returns: Sequence[float], confidence: float = 0.95, *, steps: int = 100
) -> float:
    """Return Entropic Value-at-Risk of ``returns``.

    The EVaR is approximated by numerically minimising the entropic
    bound over ``steps`` points.  The result is always positive and
    represents the maximum expected loss at the desired confidence
    level.
    """

    if len(returns) == 0:
        return 0.0
    if np is None:
        arr = [float(x) for x in returns]
        losses = [-x for x in arr]
        if all(abs(l) < 1e-12 for l in losses):
            return 0.0
        if steps <= 1:
            ts = [10 ** -3]
        else:
            ts = [10 ** (-3 + (4) * i / (steps - 1)) for i in range(steps)]
        bound = float("inf")
        for t in ts:
            mean_exp = sum(math.exp(t * l) for l in losses) / len(losses)
            val = (math.log(mean_exp) - math.log(1 - confidence)) / t
            if val < bound:
                bound = val
        return max(bound, 0.0)

    arr = np.asarray(returns, dtype=float)
    losses = -arr
    if np.allclose(losses, 0.0):
        return 0.0

    ts = np.logspace(-3, 1, steps)
    bound = float("inf")
    for t in ts:
        val = (np.log(np.mean(np.exp(t * losses))) - np.log(1 - confidence)) / t
        if val < bound:
            bound = val
    return max(bound, 0.0)


@njit
def covariance_matrix(prices: Mapping[str, Sequence[float]]) -> np.ndarray:
    """Return covariance matrix of token returns."""

    series = []
    for seq in prices.values():
        if np is None:
            arr = [float(x) for x in seq]
            if len(arr) < 2:
                continue
            rets = [arr[i + 1] / arr[i] - 1 for i in range(len(arr) - 1)]
        else:
            arr = np.asarray(seq, dtype=float)
            if len(arr) < 2:
                continue
            rets = arr[1:] / arr[:-1] - 1
        series.append(rets)
    if not series:
        return np.empty((0, 0)) if np is not None else []
    min_len = min(len(s) for s in series)
    trimmed = [s[:min_len] for s in series]
    if np is None:
        n = len(trimmed)
        means = [sum(t) / min_len for t in trimmed]
        cov = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                cov_ij = sum(
                    (trimmed[i][k] - means[i]) * (trimmed[j][k] - means[j])
                    for k in range(min_len)
                ) / min_len
                cov[i][j] = cov_ij
        return _Matrix(cov)
    mat = np.vstack(trimmed)
    return np.cov(mat)


@njit
def portfolio_cvar(
    prices: Mapping[str, Sequence[float]],
    weights: Mapping[str, float],
    confidence: float = 0.95,
) -> float:
    """Return portfolio CVaR for ``prices`` and ``weights``."""

    series = []
    w_list = []
    for tok, w in weights.items():
        seq = prices.get(tok)
        if seq is None or len(seq) < 2:
            continue
        if np is None:
            arr = [float(x) for x in seq]
            rets = [arr[i + 1] / arr[i] - 1 for i in range(len(arr) - 1)]
        else:
            arr = np.asarray(seq, dtype=float)
            rets = arr[1:] / arr[:-1] - 1
        series.append(rets)
        w_list.append(w)
    if not series:
        return 0.0
    min_len = min(len(s) for s in series)
    series = [s[:min_len] for s in series]
    if np is None:
        port_rets = [
            sum(series[j][i] * w_list[j] for j in range(len(w_list)))
            for i in range(min_len)
        ]
    else:
        mat = np.vstack(series).T
        w = np.asarray(w_list, dtype=float)
        port_rets = mat @ w
    return conditional_value_at_risk(port_rets, confidence)


@njit
def portfolio_evar(
    prices: Mapping[str, Sequence[float]],
    weights: Mapping[str, float],
    confidence: float = 0.95,
    *,
    steps: int = 100,
) -> float:
    """Return portfolio EVaR for ``prices`` and ``weights``."""

    series = []
    w_list = []
    for tok, w in weights.items():
        seq = prices.get(tok)
        if seq is None or len(seq) < 2:
            continue
        if np is None:
            arr = [float(x) for x in seq]
            rets = [arr[i + 1] / arr[i] - 1 for i in range(len(arr) - 1)]
        else:
            arr = np.asarray(seq, dtype=float)
            rets = arr[1:] / arr[:-1] - 1
        series.append(rets)
        w_list.append(w)
    if not series:
        return 0.0
    min_len = min(len(s) for s in series)
    series = [s[:min_len] for s in series]
    if np is None:
        port_rets = [
            sum(series[j][i] * w_list[j] for j in range(len(w_list)))
            for i in range(min_len)
        ]
    else:
        mat = np.vstack(series).T
        w = np.asarray(w_list, dtype=float)
        port_rets = mat @ w
    return entropic_value_at_risk(port_rets, confidence, steps=steps)


PORTFOLIO_CVAR_FUNC = portfolio_cvar
PORTFOLIO_EVAR_FUNC = portfolio_evar


def portfolio_variance(cov: np.ndarray, weights: Sequence[float]) -> float:
    """Return portfolio variance given covariance ``cov`` and ``weights``."""

    if np is None:
        if not cov:
            return 0.0
        w = [float(x) for x in weights]
        n = len(w)
        if len(cov) != n or any(len(row) != n for row in cov):
            return 0.0
        total = 0.0
        for i in range(n):
            for j in range(n):
                total += w[i] * cov[i][j] * w[j]
        return float(total)
    if cov.size == 0:
        return 0.0
    if getattr(cov, "ndim", 0) < 2:
        return 0.0
    w = np.asarray(list(weights), dtype=float)
    if cov.shape[0] != w.size or cov.shape[1] != w.size:
        return 0.0
    return float(w @ cov @ w.T)


@njit
def correlation_matrix(prices: Mapping[str, Sequence[float]]) -> np.ndarray:
    """Return correlation matrix of token returns."""
    series = []
    for seq in prices.values():
        if np is None:
            arr = [float(x) for x in seq]
            if len(arr) < 2:
                continue
            rets = [arr[i + 1] / arr[i] - 1 for i in range(len(arr) - 1)]
        else:
            arr = np.asarray(seq, dtype=float)
            if len(arr) < 2:
                continue
            rets = arr[1:] / arr[:-1] - 1
        series.append(rets)
    if not series:
        return np.empty((0, 0)) if np is not None else []
    min_len = min(len(s) for s in series)
    series = [s[:min_len] for s in series]
    if np is None:
        n = len(series)
        means = [sum(s) / min_len for s in series]
        stds = [math.sqrt(sum((s[k] - means[i]) ** 2 for k in range(min_len)) / min_len) for i, s in enumerate(series)]
        corr = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                denom = stds[i] * stds[j]
                if denom == 0:
                    corr[i][j] = 0.0
                else:
                    cov_ij = sum(
                        (series[i][k] - means[i]) * (series[j][k] - means[j])
                        for k in range(min_len)
                    ) / min_len
                    corr[i][j] = cov_ij / denom
        return _Matrix(corr)
    mat = np.vstack(series)
    return np.corrcoef(mat)


def average_correlation(prices: Mapping[str, Sequence[float]]) -> float:
    """Return average pairwise correlation for ``prices``."""

    corr = correlation_matrix(prices)
    if np is None:
        if not corr:
            return 0.0
        n = len(corr)
        if n <= 1:
            return 0.0
        total = 0.0
        for i in range(n):
            for j in range(n):
                if i != j:
                    total += corr[i][j]
        return float(total / (n * (n - 1)))
    if corr.size == 0:
        return 0.0
    n = corr.shape[0]
    if n <= 1:
        return 0.0
    off = corr - np.eye(n)
    return float(off.sum() / (n * (n - 1)))


@dataclass
class RiskManager:
    """Manage dynamic risk parameters."""

    risk_tolerance: float = 0.1
    max_allocation: float = 0.2
    max_risk_per_token: float = 0.1
    max_drawdown: float = 1.0
    volatility_factor: float = 1.0
    risk_multiplier: float = 1.0
    min_portfolio_value: float = 20.0
    funding_rate_factor: float = 1.0
    sentiment_factor: float = 1.0
    token_age_factor: float = 30.0

    @classmethod
    def from_config(cls, cfg: dict) -> "RiskManager":
        """Create ``RiskManager`` from configuration dictionary."""
        return cls(
            risk_tolerance=float(cfg.get("risk_tolerance", 0.1)),
            max_allocation=float(cfg.get("max_allocation", 0.2)),
            max_risk_per_token=float(cfg.get("max_risk_per_token", 0.1)),
            max_drawdown=float(cfg.get("max_drawdown", 1.0)),
            volatility_factor=float(cfg.get("volatility_factor", 1.0)),
            risk_multiplier=float(cfg.get("risk_multiplier", 1.0)),
            min_portfolio_value=float(cfg.get("min_portfolio_value", 20.0)),
            funding_rate_factor=float(cfg.get("funding_rate_factor", 1.0)),
            sentiment_factor=float(cfg.get("sentiment_factor", 1.0)),
            token_age_factor=float(cfg.get("token_age_factor", 30.0)),
        )

    def adjusted(
        self,
        drawdown: float = 0.0,
        volatility: float = 0.0,
        *,
        volume_spike: float = 1.0,
        depth_change: float = 0.0,
        whale_activity: float = 0.0,
        tx_rate: float = 0.0,
        tx_rate_pred: float | None = None,
        portfolio_value: float | None = None,
        funding_rate: float = 0.0,
        sentiment: float = 0.0,
        token_age: float | None = None,
        prices: Sequence[float] | None = None,
        var_threshold: float | None = None,
        var_confidence: float = 0.95,
        asset_cvar_threshold: float | None = None,
        covariance: float | None = None,
        covar_threshold: float | None = None,
        portfolio_cvar: float | None = None,
        cvar_threshold: float | None = None,
        portfolio_evar: float | None = None,
        evar_threshold: float | None = None,
        leverage: float | None = None,
        correlation: float | None = None,
        price_history: Mapping[str, Sequence[float]] | None = None,
        weights: Mapping[str, float] | None = None,
        portfolio_metrics: Mapping[str, float] | None = None,
        regime: str | None = None,
        memory: Memory | None = None,
    ) -> "RiskManager":
        """Return a new ``RiskManager`` adjusted using recent market metrics.

        Parameters
        ----------
        drawdown:
            Current portfolio drawdown as a fraction of ``max_drawdown``.
        volatility:
            Recent price volatility.
        volume_spike:
            Multiplicative factor representing sudden volume increase.
        depth_change:
            Change in order book depth from :mod:`onchain_metrics`.
        whale_activity:
            Fraction of liquidity controlled by large wallets.
        tx_rate:
            Mempool transaction rate from :mod:`onchain_metrics`.
        tx_rate_pred:
            Optional forecasted transaction rate overriding ``tx_rate`` when provided.
        portfolio_value:
            Current portfolio USD value.  When below ``min_portfolio_value`` the
            scaling factor is reduced further.
        covariance:
            Portfolio return covariance measure.  When exceeding ``covar_threshold``
            risk is scaled down.
        portfolio_cvar:
            Conditional VaR of the portfolio returns.
        portfolio_evar:
            Entropic VaR of the portfolio returns used for extreme tail
            risk management.
        leverage:
            Target leverage factor for dynamic scaling.
        correlation:
            Average correlation across held assets used for hedging.
        price_history:
            Mapping of token to historical price sequence used for covariance
            and risk calculations when ``weights`` are provided.
        var_threshold:
            When provided alongside ``prices`` the position size is scaled down
            when the token VaR exceeds this value.
        asset_cvar_threshold:
            Threshold for CVaR calculated from ``prices`` used to further limit
            position sizing.
        portfolio_metrics:
            Optional mapping containing precomputed portfolio risk metrics such
            as ``covariance`` and ``correlation``.
        weights:
            Allocation weights matching ``price_history`` tokens.
        regime:
            Optional market regime label (``"bull"``, ``"bear"`` or ``"sideways"``)
            that influences scaling.
        """

        factor = max(0.0, 1 - drawdown / self.max_drawdown)
        scale = factor / (1 + volatility * self.volatility_factor)

        cov_val = None
        cvar_val = None
        evar_val = None
        corr_val = None
        if price_history is not None and weights is not None and price_history:
            cov_matrix = covariance_matrix(price_history)
            cov_val = portfolio_variance(cov_matrix, weights.values())
            cvar_val = PORTFOLIO_CVAR_FUNC(price_history, weights, confidence=var_confidence)
            evar_val = PORTFOLIO_EVAR_FUNC(price_history, weights, confidence=var_confidence)
            corr_val = average_correlation(price_history)
        if portfolio_metrics:
            if covariance is None:
                covariance = portfolio_metrics.get("covariance")
            if portfolio_cvar is None:
                portfolio_cvar = portfolio_metrics.get("portfolio_cvar")
            if portfolio_evar is None:
                portfolio_evar = portfolio_metrics.get("portfolio_evar")
            if correlation is None:
                correlation = portfolio_metrics.get("correlation")
        if volume_spike > 1:
            scale *= min(volume_spike, 2.0)
        eff_rate = float(tx_rate_pred) if tx_rate_pred is not None else tx_rate
        if eff_rate > 1:
            scale *= min(eff_rate, 2.0)
        scale /= 1 + abs(depth_change)
        scale /= 1 + whale_activity
        scale *= self.risk_multiplier

        if funding_rate:
            if funding_rate > 0:
                scale *= 1 + funding_rate * self.funding_rate_factor
            else:
                scale /= 1 + abs(funding_rate) * self.funding_rate_factor

        if sentiment:
            scale *= 1 + sentiment * self.sentiment_factor

        if token_age is not None and self.token_age_factor > 0:
            age_scale = min(1.0, token_age / self.token_age_factor)
            scale *= age_scale

        if portfolio_value is not None and portfolio_value < self.min_portfolio_value:
            pv_scale = max(0.0, portfolio_value / self.min_portfolio_value)
            scale *= pv_scale

        if prices is not None and var_threshold is not None:
            var = value_at_risk(prices, var_confidence, memory=memory)
            if var > var_threshold and var > 0:
                scale *= var_threshold / var

        var_model_path = os.getenv("VAR_MODEL_PATH")
        if var_model_path and prices is not None:
            try:
                from .models.var_forecaster import get_model as _get_var_model
                var_model = _get_var_model(var_model_path)
            except Exception:
                var_model = None
            if var_model is not None and np is not None:
                seq_len = getattr(var_model, "seq_len", len(prices))
                if len(prices) >= seq_len:
                    seq = np.asarray(prices[-seq_len:], dtype=float)
                    predicted = float(var_model.predict(seq))
                    if var_threshold is not None and predicted > var_threshold and predicted > 0:
                        scale *= var_threshold / predicted

        if prices is not None and asset_cvar_threshold is not None:
            cvar_val_single = conditional_value_at_risk_prices(prices, var_confidence)
            if cvar_val_single > asset_cvar_threshold and cvar_val_single > 0:
                scale *= asset_cvar_threshold / cvar_val_single

        if covariance is None and cov_val is not None:
            covariance = cov_val
        if (
            covariance is not None
            and covar_threshold is not None
            and covariance > covar_threshold
        ):
            scale *= covar_threshold / covariance

        if portfolio_cvar is None and cvar_val is not None:
            portfolio_cvar = cvar_val
        if (
            portfolio_cvar is not None
            and cvar_threshold is not None
            and portfolio_cvar > cvar_threshold
        ):
            scale *= cvar_threshold / portfolio_cvar

        if portfolio_evar is None and evar_val is not None:
            portfolio_evar = evar_val
        if (
            portfolio_evar is not None
            and evar_threshold is not None
            and portfolio_evar > evar_threshold
        ):
            scale *= evar_threshold / portfolio_evar

        if correlation is None and corr_val is not None:
            correlation = corr_val
        if correlation is not None:
            corr = max(-1.0, min(1.0, correlation))
            scale *= max(0.0, 1 - corr)

        if leverage is not None and leverage > 0:
            scale *= leverage

        if regime:
            reg = regime.lower()
            if reg == "bull":
                scale *= 1.2
            elif reg == "bear":
                scale *= 0.8
        return RiskManager(
            risk_tolerance=self.risk_tolerance * scale,
            max_allocation=self.max_allocation * scale,
            max_risk_per_token=self.max_risk_per_token * scale,
            max_drawdown=self.max_drawdown,
            volatility_factor=self.volatility_factor,
            risk_multiplier=self.risk_multiplier,
            min_portfolio_value=self.min_portfolio_value,
            funding_rate_factor=self.funding_rate_factor,
            sentiment_factor=self.sentiment_factor,
            token_age_factor=self.token_age_factor,
        )
