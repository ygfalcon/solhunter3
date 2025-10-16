from __future__ import annotations
from typing import Sequence

import os

from .models.regime_classifier import get_model as _get_regime_model

from .regime_cluster import cluster_regime


def detect_regime(
    prices: Sequence[float],
    *,
    threshold: float = 0.02,
    window: int = 20,
    method: str = "kmeans",
) -> str:
    """Return market regime label.

    Parameters
    ----------
    prices:
        Historical price sequence ordered oldest to newest.
    threshold:
        Minimum fractional change over the window considered a trend when not
        enough history is available for clustering.
    window:
        Rolling window size used by the clustering model.
    method:
        Clustering algorithm to apply (``"kmeans"`` or ``"dbscan"``).
    """
    model_path = os.getenv("REGIME_MODEL_PATH")
    if model_path:
        try:
            model = _get_regime_model(model_path)
        except Exception:
            model = None
        if model is not None:
            seq_len = getattr(model, "seq_len", window)
            if len(prices) >= seq_len:
                try:
                    return model.predict(prices[-seq_len:])
                except Exception:
                    pass

    label = cluster_regime(prices, window=window, method=method)
    if label:
        return label
    if len(prices) < 2:
        return "sideways"
    change = prices[-1] / prices[0] - 1
    if change > threshold:
        return "bull"
    if change < -threshold:
        return "bear"
    return "sideways"
