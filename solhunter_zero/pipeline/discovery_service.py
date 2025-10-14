from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Iterable, Optional

from ..agents.discovery import DiscoveryAgent
from ..token_scanner import TRENDING_METADATA
from ..util.mints import clean_candidate_mints
from .types import TokenCandidate

log = logging.getLogger(__name__)


def _coerce_float(value: Any) -> float | None:
    """Best-effort conversion of ``value`` to ``float``."""

    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


class DiscoveryService:
    """Produce ``TokenCandidate`` batches for downstream scoring."""

    def __init__(
        self,
        queue: "asyncio.Queue[list[TokenCandidate]]",
        *,
        interval: float = 5.0,
        cache_ttl: float = 20.0,
        empty_cache_ttl: Optional[float] = None,
        backoff_factor: float = 2.0,
        max_backoff: Optional[float] = None,
        limit: Optional[int] = None,
        offline: bool = False,
        token_file: Optional[str] = None,
        startup_clones: Optional[int] = None,
        emit_batch_size: Optional[int] = None,
    ) -> None:
        self.queue = queue
        self.interval = max(0.1, float(interval))
        self.cache_ttl = max(0.0, float(cache_ttl))
        if empty_cache_ttl is None:
            empty_cache_ttl = self.cache_ttl
        self.empty_cache_ttl = max(0.0, float(empty_cache_ttl)) if empty_cache_ttl is not None else 0.0
        self.backoff_factor = max(1.0, float(backoff_factor))
        self.max_backoff = None if max_backoff is None else max(0.0, float(max_backoff))
        self.limit = limit
        self.offline = offline
        self.token_file = token_file
        clones_override: Optional[int] = None
        raw_clones = os.getenv("DISCOVERY_STARTUP_CLONES")
        if raw_clones:
            try:
                clones_override = int(raw_clones)
            except ValueError:
                log.warning("Invalid DISCOVERY_STARTUP_CLONES=%r; ignoring", raw_clones)
        if startup_clones is None:
            startup_clones = clones_override
        elif clones_override is not None:
            startup_clones = clones_override
        if startup_clones is None:
            startup_clones = 5
        self.startup_clones = max(1, int(startup_clones))
        batch_override: Optional[int] = None
        raw_batch = os.getenv("DISCOVERY_EMIT_BATCH_SIZE")
        if raw_batch:
            try:
                batch_override = int(raw_batch)
            except ValueError:
                log.warning("Invalid DISCOVERY_EMIT_BATCH_SIZE=%r; defaulting to single-token emission", raw_batch)
        if emit_batch_size is None:
            emit_batch_size = batch_override
        elif batch_override is not None:
            emit_batch_size = batch_override
        if emit_batch_size is None or emit_batch_size <= 0:
            emit_batch_size = 1
        self._emit_batch_size = int(emit_batch_size)
        self._agent = DiscoveryAgent()
        self._last_tokens: list[str] = []
        self._last_fetch_ts: float = 0.0
        self._cooldown_until: float = 0.0
        self._consecutive_empty: int = 0
        self._current_backoff: float = 0.0
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._last_emitted: list[str] = []
        self._last_fetch_fresh: bool = True
        self._primed = False

    async def start(self) -> None:
        if self._task is None:
            if not self._primed:
                await self._prime_startup_clones()
                self._primed = True
            self._task = asyncio.create_task(self._run(), name="discovery_service")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                tokens = await self._fetch()
                fresh = self._last_fetch_fresh
                await self._emit_tokens(tokens, fresh=fresh)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception("DiscoveryService failure: %s", exc)
            await asyncio.sleep(self.interval)

    async def _fetch(self, *, agent: DiscoveryAgent | None = None) -> list[str]:
        now = time.time()
        if now < self._cooldown_until:
            remaining = self._cooldown_until - now
            log.debug(
                "DiscoveryService cooldown active for %.2fs (last fetch yielded %d tokens)",
                remaining,
                len(self._last_tokens),
            )
            self._last_fetch_fresh = False
            return list(self._last_tokens)
        worker = agent or self._agent
        tokens = await worker.discover_tokens(
            offline=self.offline,
            token_file=self.token_file,
        )
        seq = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
        cleaned, dropped = clean_candidate_mints(seq)
        if dropped:
            log.warning(
                "DiscoveryService dropped %d invalid mint(s) at validator edge",
                len(dropped),
            )
        if self.limit:
            cleaned = cleaned[: self.limit]
        fetch_ts = time.time()
        self._apply_fetch_stats(cleaned, fetch_ts)
        return list(cleaned)

    def _build_candidates(self, tokens: Iterable[str]) -> list[TokenCandidate]:
        ts = time.time()
        result: list[TokenCandidate] = []
        for tok in tokens:
            token = str(tok)
            metadata = self._candidate_metadata(token)
            result.append(
                TokenCandidate(
                    token=token,
                    source="discovery",
                    discovered_at=ts,
                    metadata=metadata,
                )
            )
        return result

    def _candidate_metadata(self, token: str) -> Dict[str, Any]:
        """Return enriched metadata for ``token`` when available."""

        metadata: Dict[str, Any] = {}

        detail = self._agent.last_details.get(token) if hasattr(self._agent, "last_details") else None
        source_set: set[str] = set()
        if isinstance(detail, dict):
            for key, value in detail.items():
                if key == "address":
                    continue
                if key == "sources":
                    if isinstance(value, (set, list, tuple)):
                        for src in value:
                            if isinstance(src, str) and src:
                                source_set.add(src)
                    elif isinstance(value, str) and value:
                        source_set.add(value)
                    continue
                if key in {"liquidity", "volume", "price", "price_change", "market_cap", "score", "mempool_score", "helius_score"}:
                    number = _coerce_float(value)
                    if number is not None:
                        metadata[key] = number
                    continue
                metadata[key] = value

        raw = TRENDING_METADATA.get(token)
        if isinstance(raw, dict):
            for key in ("symbol", "name"):
                value = raw.get(key)
                if isinstance(value, str) and value:
                    metadata.setdefault(key, value)

            numeric_keys = {
                "price": "price",
                "volume": "volume",
                "liquidity": "liquidity",
                "market_cap": "market_cap",
                "price_change": "price_change",
            }
            for source_key, dest_key in numeric_keys.items():
                number = _coerce_float(raw.get(source_key))
                if number is not None and dest_key not in metadata:
                    metadata[dest_key] = number

            discovery_score = _coerce_float(raw.get("score"))
            if discovery_score is not None and "discovery_score" not in metadata:
                metadata["discovery_score"] = discovery_score

            sources = raw.get("sources")
            if isinstance(sources, list):
                for src in sources:
                    if isinstance(src, str) and src:
                        source_set.add(src)

            rank_value = raw.get("rank")
            try:
                if rank_value is not None and "trending_rank" not in metadata:
                    metadata["trending_rank"] = int(rank_value)
            except (TypeError, ValueError):
                pass

        method = getattr(self._agent, "last_method", None)
        if isinstance(method, str) and method:
            metadata.setdefault("discovery_method", method)
            source_set.add(f"discovery:{method}")

        if source_set:
            metadata["sources"] = sorted(source_set)

        return metadata

    async def _emit_tokens(self, tokens: Iterable[str], *, fresh: bool) -> None:
        seq = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
        seq, dropped = clean_candidate_mints(seq)
        if dropped:
            log.warning(
                "DiscoveryService dropped %d invalid mint(s) at validator edge",
                len(dropped),
            )
        if not seq:
            return
        if not fresh and seq == self._last_emitted:
            log.debug(
                "DiscoveryService skipping cached emission (%d tokens)", len(seq)
            )
            return
        total_enqueued = 0
        for chunk in self._iter_chunks(seq, self._emit_batch_size):
            batch = self._build_candidates(chunk)
            if not batch:
                continue
            await self.queue.put(batch)
            total_enqueued += len(batch)
            log.debug("DiscoveryService queued chunk of %d token(s)", len(batch))
        if total_enqueued:
            log.info("DiscoveryService queued %d token(s) across %d chunk(s)", total_enqueued, max(1, (len(seq) + self._emit_batch_size - 1) // self._emit_batch_size))
        self._last_emitted = list(seq)

    def _apply_fetch_stats(self, tokens: Iterable[str], fetch_ts: float) -> None:
        payload = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
        payload, dropped = clean_candidate_mints(payload)
        if dropped:
            log.warning(
                "DiscoveryService dropped %d invalid mint(s) at validator edge",
                len(dropped),
            )
        self._last_fetch_ts = fetch_ts
        self._last_tokens = list(payload)

        cooldown = 0.0
        if payload:
            self._consecutive_empty = 0
            self._current_backoff = 0.0
            if self.cache_ttl:
                cooldown = self.cache_ttl
        else:
            self._consecutive_empty += 1
            base_ttl = self.empty_cache_ttl
            if base_ttl:
                if self.backoff_factor > 1.0:
                    cooldown = base_ttl * (self.backoff_factor ** (self._consecutive_empty - 1))
                else:
                    cooldown = base_ttl
            self._current_backoff = cooldown

        if self.max_backoff is not None and cooldown:
            cooldown = min(cooldown, self.max_backoff)

        if cooldown:
            self._cooldown_until = fetch_ts + cooldown
            if payload:
                log.info(
                    "DiscoveryService applying cache cooldown of %.2fs after %d tokens",
                    cooldown,
                    len(payload),
                )
            else:
                log.info(
                    "DiscoveryService empty fetch #%d; backoff for %.2fs",
                    self._consecutive_empty,
                    cooldown,
                )
        else:
            self._cooldown_until = fetch_ts

        self._last_fetch_fresh = True

    async def _prime_startup_clones(self) -> None:
        clones = max(1, int(self.startup_clones))
        log.info(
            "DiscoveryService priming discovery with %d startup clone(s)", clones
        )
        tasks: list[asyncio.Task] = []
        for idx in range(clones):
            tasks.append(
                asyncio.create_task(
                    self._clone_fetch(idx), name=f"discovery_prime_{idx}"
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        aggregated: list[str] = []
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                log.warning(
                    "DiscoveryService startup clone %d failed: %s", idx, result
                )
                continue
            aggregated.extend(result)

        unique: list[str] = []
        seen: set[str] = set()
        for token in aggregated:
            tok = str(token).strip()
            if not tok or tok in seen:
                continue
            seen.add(tok)
            unique.append(tok)

        if self.limit:
            unique = unique[: self.limit]

        unique, dropped = clean_candidate_mints(unique)
        if dropped:
            log.warning(
                "DiscoveryService dropped %d invalid mint(s) at validator edge",
                len(dropped),
            )

        fetch_ts = time.time()
        self._apply_fetch_stats(unique, fetch_ts)

        if unique:
            await self._emit_tokens(unique, fresh=True)
        else:
            log.info("DiscoveryService startup clones produced no tokens")

    async def _clone_fetch(self, idx: int) -> list[str]:
        agent = DiscoveryAgent()
        try:
            tokens = await agent.discover_tokens(
                offline=self.offline,
                token_file=self.token_file,
            )
            if self.limit:
                tokens = tokens[: self.limit]
            log.debug(
                "DiscoveryService startup clone %d fetched %d tokens", idx, len(tokens)
            )
            seq = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
            cleaned, dropped = clean_candidate_mints(seq)
            if dropped:
                log.warning(
                    "DiscoveryService dropped %d invalid mint(s) at validator edge",
                    len(dropped),
                )
            return cleaned
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive logging
            log.exception("DiscoveryService startup clone %d failed", idx)
            return []

    @staticmethod
    def _iter_chunks(tokens: Iterable[str], size: int) -> Iterable[list[str]]:
        chunk: list[str] = []
        for token in tokens:
            chunk.append(token)
            if len(chunk) >= max(1, size):
                yield chunk
                chunk = []
        if chunk:
            yield chunk
