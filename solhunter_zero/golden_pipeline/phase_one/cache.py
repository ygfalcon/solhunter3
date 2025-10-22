"""Caching primitives for the Golden Stream pipeline."""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

from ..utils import canonical_hash


class BloomFilter:
    """In-memory Bloom filter used for per-cycle mint dedupe."""

    def __init__(self, *, size_bits: int = 1 << 20, hashes: int = 4) -> None:
        self._size = int(size_bits)
        self._hashes = int(hashes)
        self._array = bytearray(self._size // 8)

    def _indices(self, value: str) -> Iterable[int]:
        seed = canonical_hash(value)
        base = int(seed[:16], 16)
        for i in range(self._hashes):
            index = (base + i * 0x9E3779B97F4A7C15) % self._size
            yield int(index)

    def add(self, value: str) -> bool:
        seen = True
        for index in self._indices(value):
            byte = index // 8
            mask = 1 << (index % 8)
            if not self._array[byte] & mask:
                seen = False
                self._array[byte] |= mask
        return not seen

    def reset(self) -> None:
        self._array = bytearray(len(self._array))

    def serialise(self) -> Dict[str, Any]:
        return {"size": self._size, "hashes": self._hashes, "data": bytes(self._array).hex()}

    @classmethod
    def from_serialised(cls, payload: Mapping[str, Any]) -> "BloomFilter":
        size = int(payload.get("size", 1 << 20))
        hashes = int(payload.get("hashes", 4))
        inst = cls(size_bits=size, hashes=hashes)
        data = payload.get("data")
        if isinstance(data, str):
            inst._array = bytearray.fromhex(data)
        return inst


class TokenCache:
    """Lightweight SQLite-backed persistence for discovery and metadata state."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS token_cache (
                mint TEXT PRIMARY KEY,
                content_hash TEXT NOT NULL,
                payload BLOB,
                updated_ts REAL NOT NULL
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cursors (
                source TEXT PRIMARY KEY,
                cursor TEXT,
                updated_ts REAL NOT NULL
            )
            """
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        async with self._lock:
            self._conn.close()

    async def get_cursor(self, source: str) -> Optional[str]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT cursor FROM cursors WHERE source = ?", (source,)
            ).fetchone()
            return row[0] if row else None

    async def set_cursor(self, source: str, cursor: str) -> None:
        ts = time.time()
        async with self._lock:
            self._conn.execute(
                "REPLACE INTO cursors(source, cursor, updated_ts) VALUES(?, ?, ?)",
                (source, cursor, ts),
            )
            self._conn.commit()

    async def dump_cursors(self) -> Dict[str, str]:
        async with self._lock:
            rows = self._conn.execute("SELECT source, cursor FROM cursors").fetchall()
            return {str(source): str(cursor) for source, cursor in rows if cursor is not None}

    async def check_content(self, mint: str, content_hash: str, ttl: float) -> bool:
        cutoff = time.time() - ttl
        async with self._lock:
            row = self._conn.execute(
                "SELECT content_hash, updated_ts FROM token_cache WHERE mint = ?",
                (mint,),
            ).fetchone()
            if not row:
                return False
            stored_hash, updated_ts = row
            if float(updated_ts) < cutoff:
                return False
            return str(stored_hash) == content_hash

    async def upsert(self, mint: str, content_hash: str, payload: Mapping[str, Any]) -> None:
        ts = time.time()
        encoded = json.dumps(dict(payload), sort_keys=True)
        async with self._lock:
            self._conn.execute(
                "REPLACE INTO token_cache(mint, content_hash, payload, updated_ts) VALUES(?, ?, ?, ?)",
                (mint, content_hash, encoded, ts),
            )
            self._conn.commit()

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            rows = self._conn.execute(
                "SELECT mint, content_hash, payload, updated_ts FROM token_cache"
            ).fetchall()
            entries = []
            for mint, content_hash, payload, updated_ts in rows:
                entries.append(
                    {
                        "mint": mint,
                        "content_hash": content_hash,
                        "payload": payload,
                        "updated_ts": updated_ts,
                    }
                )
            return {"tokens": entries}

    async def load_meta_cache(self) -> Dict[str, Mapping[str, Any]]:
        async with self._lock:
            rows = self._conn.execute(
                "SELECT mint, payload FROM token_cache"
            ).fetchall()
            cache: Dict[str, Mapping[str, Any]] = {}
            for mint, payload in rows:
                try:
                    cache[str(mint)] = json.loads(payload)
                except Exception:
                    continue
            return cache


@dataclass(slots=True)
class WarmBundle:
    bloom: BloomFilter
    meta_cache: Dict[str, Mapping[str, Any]]
    cursors: Dict[str, str]
    seeds: Sequence[Mapping[str, Any]]
    buckets: Mapping[str, Mapping[str, float]]
    breakers: Mapping[str, Mapping[str, float]]

    def to_json(self) -> str:
        payload = {
            "bloom": self.bloom.serialise(),
            "meta_cache": {key: dict(value) for key, value in self.meta_cache.items()},
            "cursors": dict(self.cursors),
            "seeds": [dict(seed) for seed in self.seeds],
            "buckets": {host: dict(values) for host, values in self.buckets.items()},
            "breakers": {host: dict(values) for host, values in self.breakers.items()},
        }
        return json.dumps(payload)

    @classmethod
    def from_path(cls, path: str | os.PathLike[str]) -> Optional["WarmBundle"]:
        file = Path(path)
        if not file.exists():
            return None
        try:
            payload = json.loads(file.read_text())
        except Exception:
            return None
        bloom = BloomFilter.from_serialised(payload.get("bloom", {}))
        meta_cache = payload.get("meta_cache", {})
        cursors = payload.get("cursors", {})
        seeds = payload.get("seeds", [])
        buckets = payload.get("buckets", {})
        breakers = payload.get("breakers", {})
        return cls(
            bloom=bloom,
            meta_cache={str(k): dict(v) for k, v in meta_cache.items()},
            cursors={str(k): str(v) for k, v in cursors.items()},
            seeds=[dict(seed) for seed in seeds],
            buckets={str(k): dict(v) for k, v in buckets.items()},
            breakers={str(k): dict(v) for k, v in breakers.items()},
        )

    def persist(self, path: str | os.PathLike[str]) -> None:
        Path(path).write_text(self.to_json())
