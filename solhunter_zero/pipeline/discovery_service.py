from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, Optional, cast

from ..agents.discovery import DEFAULT_DISCOVERY_METHOD, DiscoveryAgent
from ..token_scanner import TRENDING_METADATA
from ..token_aliases import canonical_mint, validate_mint
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
        max_backoff: Optional[float] = 60.0,
        limit: Optional[int] = None,
        offline: bool = False,
        token_file: Optional[str] = None,
        startup_clones: Optional[int] = None,
        startup_clone_concurrency: Optional[int] = None,
        emit_batch_size: Optional[int] = None,
    ) -> None:
        self.queue = queue
        self.interval = max(0.1, float(interval))
        self.cache_ttl = max(0.0, float(cache_ttl))
        if empty_cache_ttl is None:
            empty_cache_ttl = self.cache_ttl
        self.empty_cache_ttl = max(0.0, float(empty_cache_ttl)) if empty_cache_ttl is not None else 0.0
        self.backoff_factor = max(1.0, float(backoff_factor))

        raw_max_backoff = os.getenv("DISCOVERY_MAX_BACKOFF")
        _env_backoff = object()
        env_max_backoff: Optional[float] | object = _env_backoff
        if raw_max_backoff is not None:
            try:
                parsed_backoff = float(raw_max_backoff)
            except ValueError:
                log.warning("Invalid DISCOVERY_MAX_BACKOFF=%r; ignoring", raw_max_backoff)
            else:
                if parsed_backoff <= 0:
                    env_max_backoff = None
                else:
                    env_max_backoff = parsed_backoff

        effective_backoff: Optional[float]
        if env_max_backoff is not _env_backoff:
            effective_backoff = cast(Optional[float], env_max_backoff)
        else:
            effective_backoff = max_backoff

        if effective_backoff is None:
            self.max_backoff = None
        else:
            capped = max(0.0, float(effective_backoff))
            self.max_backoff = None if capped == 0.0 else capped
        if limit is not None and limit < 0:
            log.warning("DiscoveryService limit %r below zero; disabling discovery", limit)
            limit = 0
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

        concurrency_override: Optional[int] = None
        raw_concurrency = os.getenv("DISCOVERY_STARTUP_CONCURRENCY")
        if raw_concurrency:
            try:
                concurrency_override = int(raw_concurrency)
            except ValueError:
                log.warning(
                    "Invalid DISCOVERY_STARTUP_CONCURRENCY=%r; ignoring",
                    raw_concurrency,
                )
        if startup_clone_concurrency is None:
            startup_clone_concurrency = concurrency_override
        elif concurrency_override is not None:
            startup_clone_concurrency = concurrency_override
        if not startup_clone_concurrency or startup_clone_concurrency <= 0:
            startup_clone_concurrency = 3
        self._startup_clone_concurrency = max(1, int(startup_clone_concurrency))
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
        self._agent = DiscoveryAgent(limit=limit)
        self._last_tokens: list[str] = []
        self._last_fetch_ts: float = 0.0
        self._cooldown_until: float = 0.0
        self._consecutive_empty: int = 0
        self._consecutive_failures: int = 0
        self._current_backoff: float = 0.0
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._last_emitted: list[str] = []
        self._last_emitted_set: frozenset[str] = frozenset()
        self._last_emitted_size: int = 0
        self._last_fetch_fresh: bool = True
        self._limit_disabled_logged = False
        self._primed = False
        self._last_metadata_fingerprints: Dict[str, str] = {}
        raw_details_cap = os.getenv("DISCOVERY_LAST_DETAILS_CAP")
        details_cap: Optional[int] | None
        if raw_details_cap in {None, ""}:
            details_cap = 4096
        else:
            try:
                parsed_cap = int(raw_details_cap)
            except ValueError:
                log.warning(
                    "Invalid DISCOVERY_LAST_DETAILS_CAP=%r; using default", raw_details_cap
                )
                details_cap = 4096
            else:
                details_cap = None if parsed_cap <= 0 else parsed_cap
        self._last_details_cap: Optional[int] = details_cap

    async def start(self) -> None:
        if self.limit == 0:
            if not self._limit_disabled_logged:
                log.info(
                    "DiscoveryService disabled (DISCOVERY_LIMIT=0); discovery loop will not start",
                )
                self._limit_disabled_logged = True
            self._stopped.set()
            self._primed = True
            return
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
                if fresh and tokens:
                    # Successful fresh fetch; clear any lingering backoff so cadence
                    # returns to the configured interval.
                    self._current_backoff = 0.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception("DiscoveryService failure: %s", exc)
                self._apply_failure_backoff(time.time())
            if self._stopped.is_set():
                break
            sleep_backoff = max(0.0, self._current_backoff)
            sleep_for = max(self.interval, sleep_backoff)
            if sleep_for <= 0:
                continue
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                raise
            else:
                if self._current_backoff > 0.0:
                    self._current_backoff = max(0.0, self._current_backoff - sleep_for)

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
        if self.limit:
            tokens = tokens[: self.limit]
        fetch_ts = time.time()
        self._apply_fetch_stats(tokens, fetch_ts)
        return list(tokens)

    def _build_candidates(
        self,
        tokens: Iterable[str],
        metadata_cache: Dict[str, Dict[str, Any]] | None = None,
    ) -> list[TokenCandidate]:
        ts = time.time()
        result: list[TokenCandidate] = []
        dropped = 0
        for tok in tokens:
            token = str(tok)
            canonical = canonical_mint(token)
            if not validate_mint(canonical):
                dropped += 1
                continue
            base_metadata: Dict[str, Any] | None = None
            if metadata_cache is not None:
                base_metadata = metadata_cache.get(canonical)
            if base_metadata is None:
                base_metadata = self._candidate_metadata(canonical)
            metadata = dict(base_metadata)
            self._ensure_discovery_tags(metadata)
            if canonical != token:
                metadata.setdefault("alias", token)
            fingerprint = self._fingerprint_metadata(metadata)
            self._last_metadata_fingerprints[canonical] = fingerprint
            result.append(
                TokenCandidate(
                    token=canonical,
                    source="discovery",
                    discovered_at=ts,
                    metadata=metadata,
                )
            )
        if dropped:
            log.debug(
                "DiscoveryService dropped %d invalid token(s) while building candidates",
                dropped,
            )
        return result

    def _ensure_discovery_tags(self, metadata: Dict[str, Any]) -> None:
        method: str | None = None
        raw_method = metadata.get("discovery_method")
        if isinstance(raw_method, str) and raw_method:
            method = raw_method
        else:
            agent_method = getattr(self._agent, "last_method", None)
            default_method = getattr(self._agent, "default_method", None)
            for candidate in (agent_method, default_method, DEFAULT_DISCOVERY_METHOD):
                if isinstance(candidate, str) and candidate:
                    method = candidate
                    break
            if method is not None:
                metadata["discovery_method"] = method

        source_set: set[str] = set()
        existing_sources = metadata.get("sources")
        if isinstance(existing_sources, (list, tuple, set, frozenset)):
            for src in existing_sources:
                if isinstance(src, str) and src:
                    source_set.add(src)
        elif isinstance(existing_sources, str) and existing_sources:
            source_set.add(existing_sources)

        if method:
            source_set.add(f"discovery:{method}")
        if not source_set and method:
            source_set.add(method)
        if not source_set:
            source_set.add("discovery")

        metadata["sources"] = sorted(source_set)

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
                if key == "depth" or key.startswith("depth_"):
                    number = _coerce_float(value)
                    if number is not None:
                        metadata[key] = number
                    elif value is not None:
                        metadata[key] = value
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

    @staticmethod
    def _normalize_metadata(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, dict):
            return {
                str(key): DiscoveryService._normalize_metadata(val)
                for key, val in sorted(value.items(), key=lambda item: str(item[0]))
            }
        if isinstance(value, list):
            return [DiscoveryService._normalize_metadata(item) for item in value]
        if isinstance(value, tuple):
            return [DiscoveryService._normalize_metadata(item) for item in value]
        if isinstance(value, (set, frozenset)):
            return sorted(
                (DiscoveryService._normalize_metadata(item) for item in value),
                key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")),
            )
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError:
                return value.hex()
        return str(value)

    def _fingerprint_metadata(self, metadata: Dict[str, Any]) -> str:
        normalized = self._normalize_metadata(metadata)
        payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def _emit_tokens(self, tokens: Iterable[str], *, fresh: bool) -> None:
        seq: list[str] = []
        dropped = 0
        seen: set[str] = set()
        for tok in tokens:
            if not isinstance(tok, str) or not tok:
                continue
            canonical = canonical_mint(tok)
            if not validate_mint(canonical):
                dropped += 1
                continue
            if canonical in seen:
                continue
            seen.add(canonical)
            seq.append(canonical)
        if not seq:
            if dropped:
                log.debug(
                    "DiscoveryService skipped %d invalid token candidate(s)", dropped
                )
            return
        seq_size = len(seq)
        seq_set = frozenset(seq)
        same_as_last = (
            seq_size == self._last_emitted_size and seq_set == self._last_emitted_set
        )
        metadata_cache: Dict[str, Dict[str, Any]] | None = None
        metadata_fingerprints: Dict[str, str] = {}
        seq_to_emit: list[str]
        if same_as_last and fresh:
            metadata_cache = {}
            changed_tokens: list[str] = []
            for token in seq:
                token_metadata = self._candidate_metadata(token)
                metadata_cache[token] = token_metadata
                fingerprint = self._fingerprint_metadata(dict(token_metadata))
                metadata_fingerprints[token] = fingerprint
                if fingerprint != self._last_metadata_fingerprints.get(token):
                    changed_tokens.append(token)
            if not changed_tokens:
                self._last_emitted = list(seq)
                self._last_emitted_set = seq_set
                self._last_emitted_size = seq_size
                self._last_metadata_fingerprints.update(metadata_fingerprints)
                log.debug(
                    "DiscoveryService skipping cached emission (%d tokens)", seq_size
                )
                return
            seq_to_emit = list(changed_tokens)
        elif same_as_last:
            log.debug(
                "DiscoveryService skipping cached emission (%d tokens)", seq_size
            )
            return
        else:
            seq_to_emit = list(seq)
        total_enqueued = 0
        for chunk in self._iter_chunks(seq_to_emit, self._emit_batch_size):
            batch = self._build_candidates(chunk, metadata_cache=metadata_cache)
            if not batch:
                continue
            missing_tags = [
                cand.token
                for cand in batch
                if not cand.metadata.get("discovery_method")
                or not cand.metadata.get("sources")
            ]
            if missing_tags:
                log.warning(
                    "DiscoveryService emitting candidates missing metadata tags: %s",
                    ", ".join(sorted(missing_tags)),
                )
            await self.queue.put(batch)
            total_enqueued += len(batch)
            log.debug("DiscoveryService queued chunk of %d token(s)", len(batch))
        if total_enqueued:
            chunk_count = max(
                1, (len(seq_to_emit) + self._emit_batch_size - 1) // self._emit_batch_size
            )
            log.info(
                "DiscoveryService queued %d token(s) across %d chunk(s)",
                total_enqueued,
                chunk_count,
            )
        if metadata_fingerprints:
            self._last_metadata_fingerprints.update(metadata_fingerprints)
        # Drop metadata fingerprints for tokens no longer present in the sequence.
        removed_tokens: list[str] = []
        for token in list(self._last_metadata_fingerprints):
            if token not in seq_set:
                self._last_metadata_fingerprints.pop(token, None)
                removed_tokens.append(token)
        if removed_tokens or (self._last_details_cap is not None and self._last_details_cap > 0):
            self._prune_agent_last_details(seq_set, removed_tokens)
        self._last_emitted = list(seq)
        self._last_emitted_set = seq_set
        self._last_emitted_size = seq_size

    def _prune_agent_last_details(
        self, current_tokens: frozenset[str], removed_tokens: Iterable[str]
    ) -> None:
        if not hasattr(self._agent, "last_details"):
            return
        details_obj = getattr(self._agent, "last_details")
        if not isinstance(details_obj, dict):
            return

        for token in removed_tokens:
            details_obj.pop(token, None)

        for token in list(details_obj):
            if token not in current_tokens:
                details_obj.pop(token, None)

        if not details_obj:
            return

        cap = self._last_details_cap
        if cap is None or cap <= 0 or len(details_obj) <= cap:
            return

        overflow = len(details_obj) - cap
        if overflow <= 0:
            return

        # Preferentially drop entries that are no longer part of the current token set.
        for token in list(details_obj):
            if overflow <= 0:
                break
            if token in current_tokens:
                continue
            details_obj.pop(token, None)
            overflow -= 1

        if overflow <= 0:
            return

        # If we still exceed the cap, drop the oldest entries regardless of membership.
        for token in list(details_obj):
            if overflow <= 0:
                break
            details_obj.pop(token, None)
            overflow -= 1

    def _apply_fetch_stats(self, tokens: Iterable[str], fetch_ts: float) -> None:
        payload = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
        self._last_fetch_ts = fetch_ts
        self._last_tokens = list(payload)
        self._consecutive_failures = 0

        cooldown = 0.0
        if payload:
            self._consecutive_empty = 0
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

        if self.max_backoff is not None and cooldown:
            cooldown = min(cooldown, self.max_backoff)

        if payload:
            self._current_backoff = 0.0
        else:
            self._current_backoff = cooldown

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

    def _apply_failure_backoff(self, failure_ts: float) -> None:
        self._consecutive_failures += 1

        cooldown = 0.0
        base_ttl = self.empty_cache_ttl
        if base_ttl:
            if self.backoff_factor > 1.0:
                cooldown = base_ttl * (self.backoff_factor ** (self._consecutive_failures - 1))
            else:
                cooldown = base_ttl

        if self.max_backoff is not None and cooldown:
            cooldown = min(cooldown, self.max_backoff)

        if cooldown:
            new_cooldown_until = failure_ts + cooldown
            if new_cooldown_until > self._cooldown_until:
                self._cooldown_until = new_cooldown_until
            self._current_backoff = cooldown
            log.warning(
                "DiscoveryService failure #%d; backoff for %.2fs",
                self._consecutive_failures,
                cooldown,
            )
        else:
            self._cooldown_until = max(self._cooldown_until, failure_ts)

        self._last_fetch_fresh = False

    async def _prime_startup_clones(self) -> None:
        if self.limit == 0:
            log.info("DiscoveryService startup clones skipped (discovery disabled)")
            return
        clones = max(1, int(self.startup_clones))
        concurrency = max(1, min(self._startup_clone_concurrency, clones))
        log.info(
            "DiscoveryService priming discovery with %d startup clone(s)", clones
        )
        tasks: list[asyncio.Task] = []

        semaphore = asyncio.Semaphore(concurrency)

        async def _run_clone(idx: int) -> tuple[list[str], Dict[str, Dict[str, Any]]]:
            async with semaphore:
                return await self._clone_fetch(idx)

        for idx in range(clones):
            tasks.append(
                asyncio.create_task(
                    _run_clone(idx), name=f"discovery_prime_{idx}"
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
            try:
                tokens, metadata = result
            except Exception:  # pragma: no cover - defensive guard
                log.warning(
                    "DiscoveryService startup clone %d returned unexpected payload", idx
                )
                continue
            aggregated.extend(tokens)
            if isinstance(metadata, dict):
                for token, detail in metadata.items():
                    if not isinstance(token, str) or not isinstance(detail, dict):
                        continue
                    normalised_token = token.strip()
                    if not normalised_token:
                        continue
                    aggregated_details[normalised_token] = dict(detail)

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

        filtered_details: Dict[str, Dict[str, Any]] = {}
        if aggregated_details:
            for token in unique:
                detail = aggregated_details.get(token)
                if isinstance(detail, dict):
                    filtered_details[token] = dict(detail)

        fetch_ts = time.time()
        self._apply_fetch_stats(unique, fetch_ts)

        if filtered_details:
            target_details: Dict[str, Dict[str, Any]]
            if hasattr(self._agent, "last_details") and isinstance(
                getattr(self._agent, "last_details"), dict
            ):
                target_details = cast(Dict[str, Dict[str, Any]], self._agent.last_details)
            else:
                target_details = {}
                setattr(self._agent, "last_details", target_details)
            for token, detail in filtered_details.items():
                existing = target_details.get(token)
                if isinstance(existing, dict):
                    merged = dict(existing)
                    merged.update(detail)
                    target_details[token] = merged
                else:
                    target_details[token] = dict(detail)

        if unique:
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
            cleaned = [str(tok) for tok in tokens if isinstance(tok, str) and tok]
            metadata: Dict[str, Dict[str, Any]] = {}
            raw_details = getattr(agent, "last_details", {})
            if isinstance(raw_details, dict):
                for token, detail in raw_details.items():
                    if not isinstance(token, str) or not isinstance(detail, dict):
                        continue
                    normalised_token = token.strip()
                    if not normalised_token:
                        continue
                    metadata[normalised_token] = dict(detail)
            return cleaned, metadata
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive logging
            log.exception("DiscoveryService startup clone %d failed", idx)
            return [], {}

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
