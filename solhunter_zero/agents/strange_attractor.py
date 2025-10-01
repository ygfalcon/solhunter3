from __future__ import annotations

import os
from typing import List, Dict, Any

import numpy as np

try:  # optional dependency
    from scipy.integrate import solve_ivp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    solve_ivp = None

try:
    import faiss  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    faiss = None

from . import BaseAgent
from ..portfolio import Portfolio


class StrangeAttractorAgent(BaseAgent):
    """Lorenz attractor strategy using past manifold overlap."""

    name = "vanta"

    def __init__(self, divergence: float = 1.0, index_path: str = "attractor.index") -> None:
        self.divergence = divergence
        self.index_path = index_path
        if faiss is not None:
            if os.path.exists(index_path):
                self.index = faiss.read_index(index_path)
            else:
                self.index = faiss.IndexIDMap2(faiss.IndexFlatL2(3))
        else:
            self.index = None
        self.labels: dict[int, str] = {}
        self._next_id = 0

    # ------------------------------------------------------------------
    def add_example(self, vector: List[float], side: str) -> None:
        if self.index is None:
            return
        vec = np.array(vector, dtype="float32")
        ids = np.array([self._next_id], dtype="int64")
        self.index.add_with_ids(np.array([vec]), ids)
        self.labels[self._next_id] = side
        self._next_id += 1

    # ------------------------------------------------------------------
    def _lorenz(self, _t: float, state: np.ndarray) -> list[float]:
        x, y, z = state
        sigma = 10.0
        rho = 28.0
        beta = 8.0 / 3.0
        dx = sigma * (y - x)
        dy = x * (rho - z) - y
        dz = x * y - beta * z
        return [dx, dy, dz]

    # ------------------------------------------------------------------
    def _compute_attractor(self, depth: float, entropy: float, velocity: float) -> np.ndarray:
        init = np.array([depth, entropy, velocity], dtype="float64")
        if solve_ivp is not None:
            res = solve_ivp(
                lambda t, s: self._lorenz(t, s),
                (0.0, 1.0),
                init,
                t_eval=[1.0],
                rtol=1e-5,
                atol=1e-8,
            )
            vec = res.y[:, -1]
        else:  # simple Euler fallback
            state = init
            dt = 0.01
            for _ in range(100):
                state = state + dt * np.array(self._lorenz(0.0, state))
            vec = state
        return vec.astype("float32")

    # ------------------------------------------------------------------
    def _query_manifold(self, vec: np.ndarray) -> Dict[str, Any] | None:
        if self.index is None or self.index.ntotal == 0:
            return None
        D, I = self.index.search(np.array([vec], dtype="float32"), 1)
        idx = int(I[0][0])
        dist = float(D[0][0])
        side = self.labels.get(idx)
        if side and dist <= self.divergence:
            overlap = 1.0 / (1.0 + dist)
            return {
                "side": side,
                "manifold_overlap": overlap,
                "divergence": dist,
                "vector": vec.tolist(),
            }
        return None

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        depth = float(depth or 0.0)
        entropy = float(imbalance or 0.0)
        velocity = float(imbalance or 0.0)
        vec = self._compute_attractor(depth, entropy, velocity)
        res = self._query_manifold(vec)
        if res is not None:
            action = {
                "token": token,
                "amount": 1.0,
                "price": 0.0,
                **res,
            }
            return [action]
        return []
