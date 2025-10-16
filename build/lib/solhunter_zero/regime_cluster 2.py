from __future__ import annotations

from typing import Sequence

import numpy as np
from sklearn.cluster import KMeans, DBSCAN


def cluster_regime(
    prices: Sequence[float],
    *,
    window: int = 20,
    method: str = "kmeans",
    n_clusters: int = 3,
    eps: float = 0.5,
    min_samples: int = 5,
) -> str | None:
    """Return regime label using clustering over returns and volatility.

    Parameters
    ----------
    prices:
        Historical price sequence ordered oldest to newest.
    window:
        Rolling window size for return and volatility features.
    method:
        ``"kmeans"`` or ``"dbscan"``.
    n_clusters:
        Number of clusters for K-means.
    eps:
        ``eps`` parameter for DBSCAN when ``method`` is ``"dbscan"``.
    min_samples:
        ``min_samples`` parameter for DBSCAN.
    """
    prices = np.asarray(list(prices), dtype=float)
    size = getattr(prices, "size", len(prices))
    if size <= window:
        return None

    returns = np.diff(prices) / prices[:-1]
    if returns.size < window:
        return None

    # rolling sum of returns and rolling volatility
    ret_sum = np.convolve(returns, np.ones(window), "valid")
    vol = np.array([
        returns[i - window + 1 : i + 1].std() for i in range(window - 1, returns.size)
    ])
    X = np.column_stack([ret_sum, vol])
    if X.shape[0] < 3:
        return None

    if method == "dbscan":
        model = DBSCAN(eps=eps, min_samples=min_samples)
        if not hasattr(model, "fit_predict"):
            return None
        labels = model.fit_predict(X)
        valid = labels != -1
        if not np.any(valid):
            return None
        centers = {
            lbl: ret_sum[labels == lbl].mean() for lbl in set(labels) if lbl != -1
        }
    else:
        model = KMeans(n_clusters=n_clusters, n_init="auto")
        if not hasattr(model, "fit_predict"):
            return None
        labels = model.fit_predict(X)
        if not hasattr(model, "cluster_centers_"):
            return None
        centers = {i: c[0] for i, c in enumerate(model.cluster_centers_)}

    sorted_labels = sorted(centers.items(), key=lambda kv: kv[1])
    bear_lbl = sorted_labels[0][0]
    bull_lbl = sorted_labels[-1][0]
    side_lbl = sorted_labels[1][0] if len(sorted_labels) > 2 else None

    lbl = labels[-1]
    if lbl == bull_lbl:
        return "bull"
    if lbl == bear_lbl:
        return "bear"
    if side_lbl is not None and lbl == side_lbl:
        return "sideways"
    return None

