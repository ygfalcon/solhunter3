from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Iterable, Optional

from ..agents.discovery import DiscoveryAgent
from ..token_scanner import TRENDING_METADATA
from .types import TokenCandidate
from ..event_bus import publish

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
        self._agent = DiscoveryAgent()
        self._last_tokens: list[str] = []
        self._last_details: Dict[str, Dict[str, Any]] = {}
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
                tokens, details = await self._fetch()
                self._last_details = details
                fresh = self._last_fetch_fresh
                await self._emit_tokens(tokens, fresh=fresh)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception("DiscoveryService failure: %s", exc)
            await asyncio.sleep(self.interval)

    async def _fetch(
        self, *, agent: DiscoveryAgent | None = None
    ) -> tuple[list[str], Dict[str, Dict[str, Any]]]:
        now = time.time()
        if now < self._cooldown_until:
            remaining = self._cooldown_until - now
            log.debug(
                "DiscoveryService cooldown active for %.2fs (last fetch yielded %d tokens)",
                remaining,
                len(self._last_tokens),
            )
            self._last_fetch_fresh = False
            return list(self._last_tokens), dict(self._last_details)
        worker = agent or self._agent
        tokens = await worker.discover_tokens(
            offline=self.offline,
            token_file=self.token_file,
        )
        if self.limit:
            tokens = tokens[: self.limit]
        fetch_ts = time.time()
        details: Dict[str, Dict[str, Any]] = {}
        raw_details = getattr(worker, "last_details", {})
        if isinstance(raw_details, dict):
            for tok in tokens:
                key = str(tok)
                payload = raw_details.get(tok)
                if not isinstance(payload, dict):
                    payload = raw_details.get(key)
                if isinstance(payload, dict):
                    details[key] = dict(payload)
        self._apply_fetch_stats(tokens, fetch_ts)
        self._last_details = dict(details)
        return list(tokens), details

    def _build_candidates(
        self,
        tokens: Iterable[str],
        *,
        details: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> list[TokenCandidate]:
        ts = time.time()
        result: list[TokenCandidate] = []
        detail_lookup = self._last_details if details is None else details
        for tok in tokens:
            token = str(tok)
            token_details = detail_lookup.get(token) if detail_lookup else None
            metadata = self._candidate_metadata(token, token_details)
            result.append(
                TokenCandidate(
                    token=token,
                    source="discovery",
                    discovered_at=ts,
                    metadata=metadata,
                )
            )
        return result

    def _candidate_metadata(
        self, token: str, details: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Return enriched metadata for ``token`` when available."""

        raw = TRENDING_METADATA.get(token)
        if not isinstance(raw, dict):
            metadata: Dict[str, Any] = {}
        else:
            metadata = self._trending_metadata(raw)

        if details:
            detail_metadata = self._detail_metadata(details)
            sources = metadata.get("sources")
            detail_sources = detail_metadata.pop("sources", [])
            if detail_sources:
                merged_sources: list[str] = []
                for seq in (sources if isinstance(sources, list) else [], detail_sources):
                    for src in seq:
                        if isinstance(src, str) and src and src not in merged_sources:
                            merged_sources.append(src)
                if merged_sources:
                    metadata["sources"] = merged_sources
            for key, value in detail_metadata.items():
                if key not in metadata:
                    metadata[key] = value

        return metadata

    def _trending_metadata(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}

        for key in ("symbol", "name"):
            value = raw.get(key)
            if isinstance(value, str) and value:
                metadata[key] = value

        numeric_keys = {
            "price": "price",
            "volume": "volume",
            "liquidity": "liquidity",
            "market_cap": "market_cap",
            "price_change": "price_change",
        }
        for source_key, dest_key in numeric_keys.items():
            number = _coerce_float(raw.get(source_key))
            if number is not None:
                metadata[dest_key] = number

        discovery_score = _coerce_float(raw.get("score"))
        if discovery_score is not None:
            metadata["discovery_score"] = discovery_score

        sources = raw.get("sources")
        if isinstance(sources, list):
            metadata["sources"] = [str(src) for src in sources if isinstance(src, str)]

        rank_value = raw.get("rank")
        try:
            if rank_value is not None:
                metadata["trending_rank"] = int(rank_value)
        except (TypeError, ValueError):
            pass

        return metadata

    def _detail_metadata(self, details: Dict[str, Any]) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}

        for key in ("symbol", "name"):
            value = details.get(key)
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
            number = _coerce_float(details.get(source_key))
            if number is not None:
                metadata.setdefault(dest_key, number)

        mempool_score = _coerce_float(details.get("mempool_score"))
        if mempool_score is None:
            mempool_score = _coerce_float(details.get("combined_score"))
        if mempool_score is None and str(details.get("source") or "").lower() == "mempool":
            mempool_score = _coerce_float(details.get("score"))
        if mempool_score is not None:
            metadata.setdefault("mempool_score", mempool_score)

        detail_score = _coerce_float(details.get("score"))
        if detail_score is not None and "mempool_score" not in metadata:
            metadata.setdefault("detail_score", detail_score)

        detail_source = details.get("detail_source")
        if isinstance(detail_source, str) and detail_source:
            metadata.setdefault("detail_source", detail_source)

        detail_sources = details.get("detail_sources")
        if isinstance(detail_sources, list):
            metadata.setdefault(
                "detail_sources",
                [
                    str(src)
                    for src in detail_sources
                    if isinstance(src, str) and src
                ],
            )

        sources: list[str] = []
        raw_sources = details.get("sources")
        if isinstance(raw_sources, list):
            sources.extend(str(src) for src in raw_sources if isinstance(src, str))
        elif isinstance(raw_sources, str) and raw_sources:
            sources.append(raw_sources)
        raw_source = details.get("source")
        if isinstance(raw_source, str) and raw_source:
            sources.append(raw_source)
        deduped_sources: list[str] = []
        for src in sources:
            if src not in deduped_sources:
                deduped_sources.append(src)
        if deduped_sources:
            metadata["sources"] = deduped_sources

        return metadata

    async def _emit_tokens(self, tokens: Iterable[str], *, fresh: bool) -> None:
        seq = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
        if not seq:
            return
        if not fresh and seq == self._last_emitted:
            log.debug(
                "DiscoveryService skipping cached emission (%d tokens)", len(seq)
            )
            return
        batch = self._build_candidates(seq)
        await self.queue.put(batch)
        log.info("DiscoveryService queued %d tokens", len(batch))
        previous = list(self._last_emitted)
        if seq and seq != previous:
            publish("token_discovered", list(seq))
        self._last_emitted = list(seq)

    def _apply_fetch_stats(self, tokens: Iterable[str], fetch_ts: float) -> None:
        payload = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
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
        aggregated_details: Dict[str, Dict[str, Any]] = {}
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                log.warning(
                    "DiscoveryService startup clone %d failed: %s", idx, result
                )
                continue
            tokens, details = result
            aggregated.extend(tokens)
            if isinstance(details, dict):
                for token, payload in details.items():
                    if isinstance(payload, dict):
                        aggregated_details[token] = dict(payload)

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

        fetch_ts = time.time()
        self._apply_fetch_stats(unique, fetch_ts)

        if unique:
            self._last_details = aggregated_details
            await self._emit_tokens(unique, fresh=True)
        else:
            log.info("DiscoveryService startup clones produced no tokens")

    async def _clone_fetch(self, idx: int) -> tuple[list[str], Dict[str, Dict[str, Any]]]:
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
            details: Dict[str, Dict[str, Any]] = {}
            raw_details = getattr(agent, "last_details", {})
            if isinstance(raw_details, dict):
                for token in seq:
                    payload = raw_details.get(token)
                    if not isinstance(payload, dict):
                        payload = raw_details.get(str(token))
                    if isinstance(payload, dict):
                        details[token] = dict(payload)
            return seq, details
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive logging
            log.exception("DiscoveryService startup clone %d failed", idx)
            return [], {}
