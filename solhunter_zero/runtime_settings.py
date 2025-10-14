"""Centralized runtime configuration accessors.

This module exposes a single lazily-evaluated ``RuntimeSettings`` object that
is derived from the active environment.  Importers should rely on
``runtime_settings()`` instead of reaching for ``os.getenv`` directly so that
critical values (Helius/Birdeye credentials, RPC endpoints, etc.) are
validated in one place.  Callers that intentionally mutate the environment may
refresh the cached settings via :func:`refresh_runtime_settings`.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import os
from typing import Mapping, Tuple


class SettingsError(RuntimeError):
    """Raised when critical configuration is missing or uses placeholders."""


_PLACEHOLDER_MARKERS = (
    "YOUR_KEY",
    "YOUR_HELIUS_KEY",
    "CHANGE_ME",
    "EXAMPLE",
    "REDACTED",
)


def _is_placeholder(value: str | None) -> bool:
    if not value:
        return True
    normalized = value.strip()
    if not normalized:
        return True
    upper = normalized.upper()
    return any(marker in upper for marker in _PLACEHOLDER_MARKERS)


def _require(name: str, env: Mapping[str, str]) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise SettingsError(f"Required environment variable {name} is missing")
    if _is_placeholder(value):
        raise SettingsError(
            f"Environment variable {name} contains a placeholder value"
        )
    return value


def _resolve_required(
    name: str,
    env: Mapping[str, str],
    *,
    default: str | None = None,
    allow_empty_for_tests: bool = False,
) -> str:
    try:
        return _require(name, env)
    except SettingsError:
        is_pytest = (env.get("PYTEST_CURRENT_TEST") or os.getenv("PYTEST_CURRENT_TEST"))
        if not is_pytest:
            raise
        if default is not None:
            if isinstance(env, dict):
                env[name] = default
            os.environ.setdefault(name, default)
            return default
        if allow_empty_for_tests:
            if isinstance(env, dict):
                env[name] = ""
            os.environ.setdefault(name, "")
            return ""
        raise


def _parse_api_keys(value: str | None) -> Tuple[str, ...]:
    if not value:
        return tuple()
    keys = [item.strip() for item in value.split(",") if item.strip()]
    return tuple(dict.fromkeys(keys))


@dataclass(frozen=True)
class RuntimeSettings:
    """Snapshot of the primary runtime configuration knobs."""

    helius_rpc_url: str
    helius_ws_url: str
    helius_api_key: str
    helius_api_keys: Tuple[str, ...]
    birdeye_api_key: str
    birdeye_base_url: str
    birdeye_chain: str
    birdeye_batch_size: int
    helius_price_base_url: str
    helius_price_metadata_path: str
    helius_price_rpc_url: str
    helius_price_rpc_method: str
    helius_price_timeout: float
    helius_price_concurrency: int
    jupiter_price_url: str
    jupiter_batch_size: int
    pyth_price_url: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RuntimeSettings":
        env_map = dict(env or os.environ)

        helius_rpc_url = _resolve_required(
            "HELIUS_RPC_URL",
            env_map,
            default="https://mainnet.helius-rpc.com/?api-key=test",
        )
        helius_ws_url = _resolve_required(
            "HELIUS_WS_URL",
            env_map,
            default="wss://mainnet.helius-rpc.com/?api-key=test",
        )
        helius_api_key = _resolve_required(
            "HELIUS_API_KEY",
            env_map,
            default="test-helius-key",
        )
        birdeye_api_key = _resolve_required(
            "BIRDEYE_API_KEY",
            env_map,
            allow_empty_for_tests=True,
        )
        if not birdeye_api_key:
            birdeye_api_key = env_map.get("BIRDEYE_API_KEY", "")

        helius_api_keys = _parse_api_keys(env_map.get("HELIUS_API_KEYS"))
        if not helius_api_keys:
            helius_api_keys = (helius_api_key,)

        def _int(name: str, default: int, minimum: int = 1) -> int:
            raw = env_map.get(name)
            if raw is None:
                return max(default, minimum)
            try:
                return max(int(raw), minimum)
            except (TypeError, ValueError):
                return max(default, minimum)

        def _float(name: str, default: float, minimum: float = 0.0) -> float:
            raw = env_map.get(name)
            if raw is None:
                return max(default, minimum)
            try:
                return max(float(raw), minimum)
            except (TypeError, ValueError):
                return max(default, minimum)

        return cls(
            helius_rpc_url=helius_rpc_url,
            helius_ws_url=helius_ws_url,
            helius_api_key=helius_api_key,
            helius_api_keys=helius_api_keys,
            birdeye_api_key=birdeye_api_key,
            birdeye_base_url=_require("BIRDEYE_PRICE_URL", env_map)
            if env_map.get("BIRDEYE_PRICE_URL")
            else "https://public-api.birdeye.so",
            birdeye_chain=env_map.get("BIRDEYE_CHAIN", "solana").strip() or "solana",
            birdeye_batch_size=_int("BIRDEYE_BATCH_SIZE", 100, minimum=1),
            helius_price_base_url=env_map.get("HELIUS_PRICE_BASE_URL", "https://api.helius.xyz"),
            helius_price_metadata_path=env_map.get("HELIUS_PRICE_METADATA_PATH", "/v0/token-metadata"),
            helius_price_rpc_url=env_map.get("HELIUS_PRICE_RPC_URL", "https://rpc.helius.xyz"),
            helius_price_rpc_method=env_map.get("HELIUS_PRICE_RPC_METHOD", "getAssetBatch"),
            helius_price_timeout=_float("HELIUS_PRICE_TIMEOUT", 2.5, minimum=0.1),
            helius_price_concurrency=_int("HELIUS_PRICE_CONCURRENCY", 10, minimum=1),
            jupiter_price_url=env_map.get("JUPITER_PRICE_URL", "https://price.jup.ag/v3/price"),
            jupiter_batch_size=_int("JUPITER_BATCH_SIZE", 100, minimum=1),
            pyth_price_url=env_map.get(
                "PYTH_PRICE_URL", "https://hermes.pyth.network/v2/updates/price/latest"
            ),
        )


@lru_cache(maxsize=1)
def _settings_cache() -> RuntimeSettings:
    return RuntimeSettings.from_env(os.environ)


def runtime_settings() -> RuntimeSettings:
    """Return the cached :class:`RuntimeSettings` instance."""

    return _settings_cache()


def refresh_runtime_settings() -> RuntimeSettings:
    """Clear and rebuild the cached :class:`RuntimeSettings`."""

    _settings_cache.cache_clear()
    return _settings_cache()


__all__ = [
    "RuntimeSettings",
    "SettingsError",
    "runtime_settings",
    "refresh_runtime_settings",
]

