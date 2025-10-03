from __future__ import annotations

from typing import List, Dict, Any

import logging

import numpy as np

try:  # optional dependency
    import pywt  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pywt = None

from . import BaseAgent
from .price_utils import resolve_price
from ..portfolio import Portfolio
from ..memory import Memory
from ..advanced_memory import AdvancedMemory
from ..offline_data import OfflineData


logger = logging.getLogger(__name__)


class FractalAgent(BaseAgent):
    """Fractal pattern matching using wavelet fingerprints."""

    name = "inferna"

    def __init__(
        self,
        similarity_threshold: float = 0.89,
        *,
        memory: Memory | AdvancedMemory | None = None,
        offline_data: OfflineData | None = None,
    ) -> None:
        self.similarity_threshold = similarity_threshold
        self.memory = memory
        self.offline_data = offline_data

    # ------------------------------------------------------------------
    def _roi_history(self, token: str) -> List[float]:
        """Return ROI series for ``token`` from stored trades."""
        trades = []
        if self.memory is not None:
            try:
                trades = self.memory.list_trades(token=token)
            except Exception:
                trades = []
        if not trades and self.offline_data is not None:
            try:
                trades = self.offline_data.list_trades(token)
            except Exception:
                trades = []
        trades.sort(key=lambda t: getattr(t, "timestamp", 0))
        rois: List[float] = []
        prev_price: float | None = None
        for t in trades:
            price = float(t.price)
            if prev_price is not None and prev_price != 0:
                rois.append((price - prev_price) / prev_price)
            prev_price = price
        return rois

    # ------------------------------------------------------------------
    def _hurst(self, series: List[float]) -> float:
        ts = np.asarray(series, dtype="float64")
        n = len(ts)
        if n < 2:
            return 0.5
        mean = np.mean(ts)
        Z = ts - mean
        Y = np.cumsum(Z)
        R = np.max(Y) - np.min(Y)
        S = np.std(ts)
        if R == 0 or S == 0:
            return 0.5
        return float(np.log(R / S) / np.log(n))

    # ------------------------------------------------------------------
    def _fractal_fingerprint(self, series: List[float]) -> np.ndarray:
        arr = np.asarray(series, dtype="float64")
        if pywt is not None and len(arr) >= 2:
            scales = np.arange(1, min(6, len(arr)))
            coeffs, _ = pywt.cwt(arr, scales, "mexh")
            feat = np.mean(np.abs(coeffs), axis=1)
        else:
            feat = arr[-5:]
        hurst = self._hurst(arr)
        return np.append(feat, hurst)

    # ------------------------------------------------------------------
    def _fractal_dimension(self, series: List[float]) -> float:
        return 2.0 - self._hurst(series)

    # ------------------------------------------------------------------
    def _past_fingerprints(self, exclude: str | None = None) -> List[np.ndarray]:
        """Return fingerprints for previously traded tokens."""
        tokens: set[str] = set()
        if self.memory is not None:
            try:
                for t in self.memory.list_trades(limit=1000):
                    tokens.add(t.token)
            except Exception:
                pass
        if self.offline_data is not None:
            try:
                for t in self.offline_data.list_trades():
                    tokens.add(t.token)
            except Exception:
                pass
        if exclude is not None:
            tokens.discard(exclude)
        fps: List[np.ndarray] = []
        for tok in tokens:
            history = self._roi_history(tok)
            if len(history) >= 2:
                fps.append(self._fractal_fingerprint(history))
        return fps

    # ------------------------------------------------------------------
    def _hausdorff(self, a: np.ndarray, b: np.ndarray) -> float:
        a = np.atleast_2d(a.astype("float64"))
        b = np.atleast_2d(b.astype("float64"))
        dist_ab = max(np.min(np.linalg.norm(ai - b, axis=1)) for ai in a)
        dist_ba = max(np.min(np.linalg.norm(bi - a, axis=1)) for bi in b)
        return float(max(dist_ab, dist_ba))

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        history = self._roi_history(token)
        if len(history) < 2:
            return []
        fp = self._fractal_fingerprint(history)
        dim = self._fractal_dimension(history)

        past = self._past_fingerprints(exclude=token)
        best_sim = 0.0
        best_shift = 0.0
        best_idx = -1
        for idx, other in enumerate(past):
            dist = self._hausdorff(fp, other)
            sim = 1.0 / (1.0 + dist)
            if sim > best_sim:
                best_sim = sim
                best_shift = fp[-1] - other[-1]
                best_idx = idx

        if best_sim >= self.similarity_threshold and 1.58 <= dim <= 1.63:
            price, context = await resolve_price(token, portfolio)
            if price <= 0:
                logger.info(
                    "%s agent skipping buy for %s due to missing price: %s",
                    self.name,
                    token,
                    context,
                )
                return []
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": 1.0,
                    "price": price,
                    "fractal_overlap_score": best_sim,
                    "hurst_shift_vector": best_shift,
                    "ghost_transfer_index": best_idx,
                    "recursive_alpha_magnification": best_sim * dim,
                }
            ]
        return []
