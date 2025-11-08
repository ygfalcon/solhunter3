"""Lightweight metrics instrumentation helpers.

This module provides a minimal interface that can be used throughout the code
base without coupling discovery to a specific metrics backend.  The default
implementation is a no-op, but if the ``STATSD_HOST`` environment variable is
set *and* the ``statsd`` package is available we will emit metrics using
StatsD-compatible semantics.  The helpers are intentionally tiny so they can be
swapped for Prometheus, Datadog, etc. in deployment specific entry-points.
"""

from __future__ import annotations

import contextlib
import os
import time
from typing import Mapping, Optional

try:  # pragma: no cover - optional dependency
    from statsd import StatsClient  # type: ignore
except Exception:  # pragma: no cover - statsd is optional
    StatsClient = None  # type: ignore

__all__ = [
    "MetricsClient",
    "NullMetricsClient",
    "get_metrics",
    "set_metrics_client",
]


def _normalise_tag(value: object) -> str:
    text = str(value).strip().replace(" ", "_")
    if not text:
        return "unknown"
    return text.replace("/", "-")


def _format_metric_name(name: str, tags: Optional[Mapping[str, object]]) -> str:
    if not tags:
        return name
    parts = [name]
    for key in sorted(tags):
        parts.append(f"{key}.{_normalise_tag(tags[key])}")
    return ".".join(parts)


class MetricsClient:
    """Abstract metrics sink used by discovery components."""

    def increment(
        self, name: str, value: float = 1.0, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        raise NotImplementedError

    def gauge(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        raise NotImplementedError

    def observe(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        """Record a timing/latency sample in seconds."""

        raise NotImplementedError

    @contextlib.contextmanager
    def time(self, name: str, *, tags: Optional[Mapping[str, object]] = None):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.observe(name, time.perf_counter() - start, tags=tags)


class NullMetricsClient(MetricsClient):
    """Metrics sink that performs no operations."""

    def increment(
        self, name: str, value: float = 1.0, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:  # noqa: D401 - trivial doc
        return

    def gauge(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:  # noqa: D401 - trivial doc
        return

    def observe(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:  # noqa: D401 - trivial doc
        return


class _StatsDMetricsClient(MetricsClient):  # pragma: no cover - optional backend
    def __init__(self, client: StatsClient, *, prefix: str | None = None) -> None:
        self._client = client
        self._prefix = prefix.rstrip(".") if prefix else ""

    def _name(self, name: str, tags: Optional[Mapping[str, object]]) -> str:
        metric = _format_metric_name(name, tags)
        if not self._prefix:
            return metric
        return f"{self._prefix}.{metric}"

    def increment(
        self, name: str, value: float = 1.0, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        metric = self._name(name, tags)
        self._client.incr(metric, value)

    def gauge(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        metric = self._name(name, tags)
        self._client.gauge(metric, value)

    def observe(
        self, name: str, value: float, *, tags: Optional[Mapping[str, object]] = None
    ) -> None:
        metric = self._name(name, tags)
        # StatsD timing expects milliseconds.
        self._client.timing(metric, value * 1000.0)


_METRICS_CLIENT: MetricsClient = NullMetricsClient()


def _build_default_client() -> MetricsClient:
    if StatsClient is None:
        return NullMetricsClient()
    host = os.getenv("STATSD_HOST")
    if not host:
        return NullMetricsClient()
    port = int(os.getenv("STATSD_PORT", "8125") or 8125)
    prefix = os.getenv("STATSD_PREFIX", "solhunter")
    try:
        client = StatsClient(host=host, port=port, prefix=None)
    except Exception:
        return NullMetricsClient()
    return _StatsDMetricsClient(client, prefix=prefix)


def get_metrics() -> MetricsClient:
    """Return the process-wide metrics client."""

    global _METRICS_CLIENT
    if isinstance(_METRICS_CLIENT, NullMetricsClient):
        # Lazy initialisation so tests can override before first use.
        _METRICS_CLIENT = _build_default_client()
    return _METRICS_CLIENT


def set_metrics_client(client: Optional[MetricsClient]) -> None:
    """Override the global metrics client for testing or custom sinks."""

    global _METRICS_CLIENT
    _METRICS_CLIENT = client or NullMetricsClient()
