import time
import datetime as dt
from types import SimpleNamespace

import numpy as np


def _build_old(trades, snaps):
    start = time.time()
    trade_list = list(trades)
    n = len(trade_list)
    tokens = np.array([t.token for t in trade_list])
    timestamps = np.array([
        getattr(t, "timestamp", dt.datetime.utcnow()).timestamp()
        for t in trade_list
    ], dtype=np.float64)

    snap_map = {}
    for s in snaps:
        snap_map.setdefault(s.token, []).append(s)
    snap_data = {}
    for token, seq in snap_map.items():
        seq.sort(key=lambda x: x.timestamp)
        snap_data[token] = {
            "ts": np.array([s.timestamp.timestamp() for s in seq], dtype=np.float64),
            "depth": np.array([float(getattr(s, "depth", 0.0)) for s in seq], dtype=np.float32),
            "slippage": np.array([float(getattr(s, "slippage", 0.0)) for s in seq], dtype=np.float32),
            "imbalance": np.array([float(getattr(s, "imbalance", 0.0)) for s in seq], dtype=np.float32),
            "tx_rate": np.array([float(getattr(s, "tx_rate", 0.0)) for s in seq], dtype=np.float32),
        }

    depth = np.zeros(n, dtype=np.float32)
    for token, data in snap_data.items():
        mask = tokens == token
        if not np.any(mask):
            continue
        tts = timestamps[mask]
        idx = np.searchsorted(data["ts"], tts, side="right") - 1
        idx[idx < 0] = 0
        depth[mask] = data["depth"][idx]
    return time.time() - start


def _build_new(trades, snaps):
    start = time.time()
    trade_list = list(trades)
    n = len(trade_list)
    tokens = np.array([t.token for t in trade_list], dtype=object)
    timestamps = np.array([
        getattr(t, "timestamp", dt.datetime.utcnow()).timestamp() for t in trade_list
    ], dtype=np.float64)

    snap_tokens = []
    snap_ts = []
    snap_depth = []
    for s in snaps:
        snap_tokens.append(s.token)
        snap_ts.append(s.timestamp.timestamp())
        snap_depth.append(float(getattr(s, "depth", 0.0)))
    snap_tokens = np.array(snap_tokens, dtype=object)
    snap_ts = np.array(snap_ts, dtype=np.float64)
    snap_depth = np.array(snap_depth, dtype=np.float32)

    token_map = {t: i for i, t in enumerate(np.unique(snap_tokens))}
    snap_tok_idx = np.array([token_map[t] for t in snap_tokens])
    trade_tok_idx = np.array([token_map.get(t, -1) for t in tokens])

    shift = np.max(snap_ts) + 1.0
    encoded_snaps = snap_tok_idx * shift + snap_ts
    order = np.argsort(encoded_snaps)
    encoded_snaps = encoded_snaps[order]
    snap_depth = snap_depth[order]

    encoded_trades = trade_tok_idx * shift + timestamps
    idx = np.searchsorted(encoded_snaps, encoded_trades, side="right") - 1
    idx[idx < 0] = 0
    valid = trade_tok_idx >= 0
    depth = np.zeros(n, dtype=np.float32)
    depth[valid] = snap_depth[idx[valid]]
    return time.time() - start


def test_dataset_build_speed():
    now = dt.datetime.utcnow()
    trades = [SimpleNamespace(token=f"tok{i%5}", price=1.0, amount=1.0, timestamp=now)
              for i in range(100000)]
    snaps = [SimpleNamespace(token=f"tok{i%5}", depth=1.0, slippage=0.1, imbalance=0.0,
                             tx_rate=0.0, timestamp=now)
             for i in range(50000)]

    old_time = _build_old(trades, snaps)
    new_time = _build_new(trades, snaps)
    assert new_time <= old_time * 1.2

