"""Startup helpers for SolHunter Zero."""

from __future__ import annotations

import time

from .config import (
    apply_env_overrides,
    load_config,
    set_env_from_config,
    initialize_event_bus,
)
from .config_runtime import Config
from . import metrics_aggregator
from .connectivity import ensure_connectivity


def prepare_environment(
    config_path: str | None,
    *,
    offline: bool = False,
    dry_run: bool = False,
) -> tuple[dict, Config]:
    """Load configuration, set environment variables and verify connectivity."""
    start = time.perf_counter()
    cfg = apply_env_overrides(load_config(config_path))
    set_env_from_config(cfg)
    initialize_event_bus()
    runtime_cfg = Config.from_env(cfg)
    metrics_aggregator.publish(
        "startup_config_load_duration", time.perf_counter() - start
    )

    start = time.perf_counter()
    ensure_connectivity(offline=offline or dry_run)
    metrics_aggregator.publish(
        "startup_connectivity_check_duration", time.perf_counter() - start
    )

    return cfg, runtime_cfg

