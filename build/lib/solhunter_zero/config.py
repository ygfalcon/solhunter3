# solhunter_zero/config.py
from __future__ import annotations

import ast
import os
import sys
import logging
import re
from typing import Mapping, Any, Sequence, cast
from pathlib import Path
from importlib import import_module
import urllib.parse

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - fallback for <3.11
    try:
        import tomli as tomllib  # type: ignore[import-not-found, no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        tomllib = None  # type: ignore[assignment]
from pydantic import ValidationError

from .jsonutil import loads, dumps
from .dex_config import DEXConfig
from .paths import ROOT
from .config_schema import ConfigModel

DEFAULT_HELIUS_RPC_URL = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
DEFAULT_HELIUS_WS_URL = "wss://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL helpers / validation
# ---------------------------------------------------------------------------

_VALIDATED_URLS: dict[str, str] = {}


_REDACT_PATTERN = re.compile(r"(?i)(api[-_]?key|token|auth)=([^&#]+)")


def _redact(text: str | None) -> str:
    """Mask sensitive query parameter values when logging URLs."""

    if text is None:
        return ""

    def _mask(match: re.Match[str]) -> str:
        return f"{match.group(1)}=****"

    return _REDACT_PATTERN.sub(_mask, text)


def _extract_helius_api_key(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        keys = params.get("api-key") or params.get("api_key")
        if keys:
            for item in reversed(keys):
                if item:
                    return str(item)
    except Exception:
        return None
    return None


def _validate_and_store_url(name: str, url: str, schemes: set[str] | None = None) -> str:
    """
    Validate a URL (preserving path & query) and store in env + cache.

    - Ensures scheme + netloc exist.
    - Restricts to allowed schemes when provided.
    """
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid {name}: {_redact(url)!r}")
    if schemes and parsed.scheme not in schemes:
        raise ValueError(
            f"Invalid {name} scheme: {parsed.scheme} ({_redact(url)!r})"
        )
    # Persist exactly as provided (keep query strings like ?api-key=…)
    os.environ[name] = url
    _VALIDATED_URLS[name] = url
    return url


try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None


# ---------------------------------------------------------------------------
# Env mapping (config keys -> env var names)
# ---------------------------------------------------------------------------

ENV_VARS = {
    "birdeye_api_key": "BIRDEYE_API_KEY",
    "solana_rpc_url": "SOLANA_RPC_URL",
    "solana_ws_url": "SOLANA_WS_URL",
    "solana_keypair": "SOLANA_KEYPAIR",
    "helius_rpc_url": "HELIUS_RPC_URL",
    "helius_ws_url": "HELIUS_WS_URL",
    "helius_api_key": "HELIUS_API_KEY",
    "helius_api_keys": "HELIUS_API_KEYS",
    "helius_api_token": "HELIUS_API_TOKEN",
    "helius_price_rpc_url": "HELIUS_PRICE_RPC_URL",
    "helius_price_rpc_method": "HELIUS_PRICE_RPC_METHOD",
    "helius_price_single_method": "HELIUS_PRICE_SINGLE_METHOD",
    "helius_price_rest_url": "HELIUS_PRICE_REST_URL",
    "helius_price_base_url": "HELIUS_PRICE_BASE_URL",
    "helius_price_metadata_path": "HELIUS_PRICE_METADATA_PATH",
    "helius_price_timeout": "HELIUS_PRICE_TIMEOUT",
    "helius_price_concurrency": "HELIUS_PRICE_CONCURRENCY",
    "dex_base_url": "DEX_BASE_URL",
    "dex_testnet_url": "DEX_TESTNET_URL",
    "orca_api_url": "ORCA_API_URL",
    "raydium_api_url": "RAYDIUM_API_URL",
    "phoenix_api_url": "PHOENIX_API_URL",
    "meteora_api_url": "METEORA_API_URL",
    "orca_ws_url": "ORCA_WS_URL",
    "raydium_ws_url": "RAYDIUM_WS_URL",
    "phoenix_ws_url": "PHOENIX_WS_URL",
    "meteora_ws_url": "METEORA_WS_URL",
    "jupiter_ws_url": "JUPITER_WS_URL",
    "orca_dex_url": "ORCA_DEX_URL",
    "raydium_dex_url": "RAYDIUM_DEX_URL",
    "phoenix_dex_url": "PHOENIX_DEX_URL",
    "meteora_dex_url": "METEORA_DEX_URL",
    "metrics_base_url": "METRICS_BASE_URL",
    "metrics_url": "METRICS_URL",
    "news_feeds": "NEWS_FEEDS",
    "twitter_feeds": "TWITTER_FEEDS",
    "discord_feeds": "DISCORD_FEEDS",
    "discovery_method": "DISCOVERY_METHOD",
    "risk_tolerance": "RISK_TOLERANCE",
    "max_allocation": "MAX_ALLOCATION",
    "max_risk_per_token": "MAX_RISK_PER_TOKEN",
    "risk_multiplier": "RISK_MULTIPLIER",
    "trailing_stop": "TRAILING_STOP",
    "max_drawdown": "MAX_DRAWDOWN",
    "volatility_factor": "VOLATILITY_FACTOR",
    "arbitrage_threshold": "ARBITRAGE_THRESHOLD",
    "arbitrage_amount": "ARBITRAGE_AMOUNT",
    "min_portfolio_value": "MIN_PORTFOLIO_VALUE",
    "min_delay": "MIN_DELAY",
    "max_delay": "MAX_DELAY",
    "offline_data_limit_gb": "OFFLINE_DATA_LIMIT_GB",
    "strategies": "STRATEGIES",
    # NOTE: legacy token filters are still accepted via env but not required by code
    "token_suffix": "TOKEN_SUFFIX",
    "token_keywords": "TOKEN_KEYWORDS",
    "token_exclude_keywords": "TOKEN_EXCLUDE_KEYWORDS",
    "volume_threshold": "VOLUME_THRESHOLD",
    "discovery_min_volume_usd": "DISCOVERY_MIN_VOLUME_USD",
    "discovery_min_liquidity_usd": "DISCOVERY_MIN_LIQUIDITY_USD",
    "discovery_max_tokens": "DISCOVERY_MAX_TOKENS",
    "discovery_overfetch_factor": "DISCOVERY_OVERFETCH_FACTOR",
    "discovery_cache_ttl": "DISCOVERY_CACHE_TTL",
    "discovery_mempool_limit": "DISCOVERY_MEMPOOL_LIMIT",
    "discovery_volume_weight": "DISCOVERY_VOLUME_WEIGHT",
    "discovery_liquidity_weight": "DISCOVERY_LIQUIDITY_WEIGHT",
    "discovery_mempool_bonus": "DISCOVERY_MEMPOOL_BONUS",
    "llm_model": "LLM_MODEL",
    "llm_context_length": "LLM_CONTEXT_LENGTH",
    "agents": "AGENTS",
    "agent_weights": "AGENT_WEIGHTS",
    "weights_path": "WEIGHTS_PATH",
    "dex_priorities": "DEX_PRIORITIES",
    "dex_partner_urls": "DEX_PARTNER_URLS",
    "dex_swap_paths": "DEX_SWAP_PATHS",
    "dex_fees": "DEX_FEES",
    "dex_gas": "DEX_GAS",
    "dex_latency": "DEX_LATENCY",
    "dex_latency_refresh_interval": "DEX_LATENCY_REFRESH_INTERVAL",
    "priority_fees": "PRIORITY_FEES",
    "priority_rpc": "PRIORITY_RPC",
    "jito_rpc_url": "JITO_RPC_URL",
    "jito_auth": "JITO_AUTH",
    "jito_ws_url": "JITO_WS_URL",
    "jito_ws_auth": "JITO_WS_AUTH",
    "event_bus_url": "EVENT_BUS_URL",
    "event_bus_peers": "EVENT_BUS_PEERS",
    "broker_url": "BROKER_URL",
    "broker_urls": "BROKER_URLS",
    "broker_heartbeat_interval": "BROKER_HEARTBEAT_INTERVAL",
    "broker_retry_limit": "BROKER_RETRY_LIMIT",
    "compress_events": "COMPRESS_EVENTS",
    "event_serialization": "EVENT_SERIALIZATION",
    "event_batch_ms": "EVENT_BATCH_MS",
    "event_mmap_batch_ms": "EVENT_MMAP_BATCH_MS",
    "event_mmap_batch_size": "EVENT_MMAP_BATCH_SIZE",
    "order_book_ws_url": "ORDER_BOOK_WS_URL",
    "depth_service": "DEPTH_SERVICE",
    "depth_max_restarts": "DEPTH_MAX_RESTARTS",
    "use_depth_stream": "USE_DEPTH_STREAM",
    "use_depth_feed": "USE_DEPTH_FEED",
    "use_rust_exec": "USE_RUST_EXEC",
    "use_service_exec": "USE_SERVICE_EXEC",
    "use_service_route": "USE_SERVICE_ROUTE",
    "mempool_score_threshold": "MEMPOOL_SCORE_THRESHOLD",
    "mempool_stats_window": "MEMPOOL_STATS_WINDOW",
    "use_flash_loans": "USE_FLASH_LOANS",
    "max_flash_amount": "MAX_FLASH_AMOUNT",
    "flash_loan_ratio": "FLASH_LOAN_RATIO",
    "use_mev_bundles": "USE_MEV_BUNDLES",
    "mempool_threshold": "MEMPOOL_THRESHOLD",
    "bundle_size": "BUNDLE_SIZE",
    "depth_threshold": "DEPTH_THRESHOLD",
    "depth_update_threshold": "DEPTH_UPDATE_THRESHOLD",
    "depth_min_send_interval": "DEPTH_MIN_SEND_INTERVAL",
    "cpu_low_threshold": "CPU_LOW_THRESHOLD",
    "cpu_high_threshold": "CPU_HIGH_THRESHOLD",
    "max_concurrency": "MAX_CONCURRENCY",
    "cpu_usage_threshold": "CPU_USAGE_THRESHOLD",
    "concurrency_smoothing": "CONCURRENCY_SMOOTHING",
    "concurrency_kp": "CONCURRENCY_KP",
    "concurrency_ki": "CONCURRENCY_KI",
    "min_rate": "MIN_RATE",
    "max_rate": "MAX_RATE",
    "depth_freq_low": "DEPTH_FREQ_LOW",
    "depth_freq_high": "DEPTH_FREQ_HIGH",
    "max_hops": "MAX_HOPS",
    "path_algorithm": "PATH_ALGORITHM",
    "use_gnn_routing": "USE_GNN_ROUTING",
    "gnn_model_path": "GNN_MODEL_PATH",
    "offline_data_interval": "OFFLINE_DATA_INTERVAL",
    "gpu_memory_index": "GPU_MEMORY_INDEX",
    "memory_sync_interval": "MEMORY_SYNC_INTERVAL",
    "memory_snapshot_path": "MEMORY_SNAPSHOT_PATH",
    "use_gpu_sim": "USE_GPU_SIM",
    "rl_build_mmap_dataset": "RL_BUILD_MMAP_DATASET",
    "rl_prefetch_buffer": "RL_PREFETCH_BUFFER",
    "live_discovery": "LIVE_DISCOVERY",
}


# ---------------------------------------------------------------------------
# Websocket derivation
# ---------------------------------------------------------------------------

def get_solana_ws_url() -> str | None:
    """
    Return a valid websocket URL for Solana RPC.

    If SOLANA_WS_URL is missing or is http(s), derive from SOLANA_RPC_URL
    by replacing scheme http->ws and https->wss.
    """
    if "SOLANA_WS_URL" in _VALIDATED_URLS:
        if _VALIDATED_URLS["SOLANA_WS_URL"]:
            os.environ.setdefault("SOLANA_WS_URL", _VALIDATED_URLS["SOLANA_WS_URL"])
        return _VALIDATED_URLS["SOLANA_WS_URL"] or None

    url = os.getenv("SOLANA_WS_URL")

    if not url:
        rpc = os.getenv("SOLANA_RPC_URL") or DEFAULT_HELIUS_RPC_URL
        if rpc:
            # preserve host/path/query while flipping scheme
            parsed = urllib.parse.urlparse(rpc)
            scheme = "wss" if parsed.scheme == "https" else ("ws" if parsed.scheme == "http" else parsed.scheme)
            if scheme in {"ws", "wss"}:
                url = urllib.parse.urlunparse(parsed._replace(scheme=scheme))
    else:
        if url.startswith("http://"):
            url = "ws://" + url[len("http://"):]
        elif url.startswith("https://"):
            url = "wss://" + url[len("https://"):]

    if url:
        try:
            return _validate_and_store_url("SOLANA_WS_URL", url, {"ws", "wss"})
        except ValueError as exc:
            logger.error(str(exc))
            return None

    try:
        return _validate_and_store_url(
            "SOLANA_WS_URL", DEFAULT_HELIUS_WS_URL, {"ws", "wss"}
        )
    except ValueError as exc:  # pragma: no cover - defaults are static
        logger.error(str(exc))
        return None


def _ensure_helius_environment(config: Mapping[str, Any]) -> None:
    """Propagate user supplied Helius credentials and endpoints into the runtime."""

    def _first_str(values: Sequence[object]) -> str | None:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _should_update(name: str, default: str | None = None) -> bool:
        current = os.getenv(name)
        if not current:
            return True
        if default and current.strip() == default.strip():
            return True
        return False

    cfg_rpc = _first_str(
        [config.get("helius_rpc_url"), config.get("solana_rpc_url"), config.get("priority_rpc")]
    )
    env_rpc = _first_str([os.getenv("SOLANA_RPC_URL"), os.getenv("HELIUS_RPC_URL")])

    if cfg_rpc and _should_update("HELIUS_RPC_URL", DEFAULT_HELIUS_RPC_URL):
        os.environ["HELIUS_RPC_URL"] = cfg_rpc
    elif env_rpc and not os.getenv("HELIUS_RPC_URL"):
        os.environ["HELIUS_RPC_URL"] = env_rpc

    if cfg_rpc and _should_update("SOLANA_RPC_URL", DEFAULT_HELIUS_RPC_URL):
        try:
            _validate_and_store_url("SOLANA_RPC_URL", cfg_rpc, {"http", "https"})
        except ValueError as exc:
            logger.error(str(exc))
    elif env_rpc and not os.getenv("SOLANA_RPC_URL"):
        try:
            _validate_and_store_url("SOLANA_RPC_URL", env_rpc, {"http", "https"})
        except ValueError as exc:
            logger.error(str(exc))

    cfg_ws = _first_str([config.get("helius_ws_url"), config.get("solana_ws_url")])
    env_ws = _first_str([os.getenv("SOLANA_WS_URL"), os.getenv("HELIUS_WS_URL")])

    if cfg_ws and _should_update("HELIUS_WS_URL", DEFAULT_HELIUS_WS_URL):
        try:
            _validate_and_store_url("HELIUS_WS_URL", cfg_ws, {"ws", "wss"})
        except ValueError as exc:
            logger.error(str(exc))
    elif env_ws and not os.getenv("HELIUS_WS_URL"):
        try:
            _validate_and_store_url("HELIUS_WS_URL", env_ws, {"ws", "wss"})
        except ValueError as exc:
            logger.error(str(exc))

    if cfg_ws and _should_update("SOLANA_WS_URL", DEFAULT_HELIUS_WS_URL):
        try:
            _validate_and_store_url("SOLANA_WS_URL", cfg_ws, {"ws", "wss"})
        except ValueError as exc:
            logger.error(str(exc))
    elif env_ws and not os.getenv("SOLANA_WS_URL"):
        try:
            _validate_and_store_url("SOLANA_WS_URL", env_ws, {"ws", "wss"})
        except ValueError as exc:
            logger.error(str(exc))

    key_candidate = _first_str(
        [
            os.getenv("HELIUS_API_KEY"),
            config.get("helius_api_key"),
            config.get("helius_api_token"),
            _extract_helius_api_key(os.getenv("SOLANA_RPC_URL")),
            _extract_helius_api_key(os.getenv("SOLANA_WS_URL")),
            _extract_helius_api_key(os.getenv("HELIUS_RPC_URL")),
            _extract_helius_api_key(os.getenv("HELIUS_WS_URL")),
        ]
    )

    if key_candidate and (not os.getenv("HELIUS_API_KEY") or not os.getenv("HELIUS_API_KEY").strip()):
        os.environ["HELIUS_API_KEY"] = key_candidate

    multi = config.get("helius_api_keys")
    if (
        multi
        and isinstance(multi, (list, tuple, set))
        and (_should_update("HELIUS_API_KEYS") or not os.getenv("HELIUS_API_KEYS"))
    ):
        joined = ",".join(str(item).strip() for item in multi if str(item).strip())
        if joined:
            os.environ["HELIUS_API_KEYS"] = joined

# Commonly required environment variables
REQUIRED_ENV_VARS = (
    "EVENT_BUS_URL",
    "SOLANA_RPC_URL",
    "SOLANA_KEYPAIR",
)


class ConfigFileNotFound(FileNotFoundError):
    """Raised when a configuration file cannot be located."""


# ---------------------------------------------------------------------------
# Internal publish helper
# ---------------------------------------------------------------------------

def _publish(topic: str, payload: Any, *, dedupe_key: str | None = None) -> None:
    """Proxy to :func:`event_bus.publish` imported lazily."""
    ev = import_module("solhunter_zero.event_bus")
    ev.publish(topic, payload, dedupe_key=dedupe_key)


# ---------------------------------------------------------------------------
# Config file loading
# ---------------------------------------------------------------------------

def _read_config_file(path: Path) -> dict:
    """Return configuration dictionary from a YAML or TOML file."""
    if path.suffix in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required for YAML config files")
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    if path.suffix == ".toml":
        if tomllib is None:
            raise RuntimeError(
                "TOML support requires Python 3.11+ or installing the optional 'tomli' package"
            )
        with path.open("rb") as fh:
            return tomllib.load(fh)
    raise ValueError(f"Unsupported config format: {path}")


def _apply_helius_defaults(cfg: dict[str, Any]) -> None:
    """Replace placeholder Helius credentials with usable defaults."""

    def _sanitize(url: Any, default: str) -> str | None:
        """Return ``default`` when ``url`` is empty or still a placeholder."""
        if not isinstance(url, str):
            return None
        text = url.strip()
        if not text:
            return default
        for marker in ("YOUR_KEY", "YOUR_HELIUS_KEY"):
            if marker in text:
                return default
        return None

    rpc_default = _sanitize(cfg.get("solana_rpc_url"), DEFAULT_HELIUS_RPC_URL)
    if rpc_default:
        cfg["solana_rpc_url"] = rpc_default
        cfg.setdefault("helius_rpc_url", rpc_default)
        cfg.setdefault("priority_rpc", rpc_default)

    ws_default = _sanitize(cfg.get("solana_ws_url"), DEFAULT_HELIUS_WS_URL)
    if ws_default:
        cfg["solana_ws_url"] = ws_default
        cfg.setdefault("helius_ws_url", ws_default)

    api_key = cfg.get("helius_api_key") or cfg.get("helius_api_token")
    if not api_key:
        extracted = _extract_helius_api_key(cfg.get("solana_rpc_url"))
        if not extracted:
            extracted = _extract_helius_api_key(cfg.get("helius_rpc_url"))
        if extracted:
            cfg.setdefault("helius_api_key", extracted)
def load_config(path: str | os.PathLike | None = None) -> dict:
    """Load configuration from `path` or default locations, validate via Pydantic."""
    if path is None:
        path = find_config_file()
    if path is None:
        raise ConfigFileNotFound(
            "Configuration file not found. Set SOLHUNTER_CONFIG or create config.toml"
        )
    p = Path(path)
    if not p.exists():
        raise ConfigFileNotFound(str(p))
    cfg = _read_config_file(p)
    try:
        model = ConfigModel(**cfg)
        cfg = model.model_dump(mode="json") if hasattr(model, "model_dump") else model.dict()
    except ValidationError as exc:
        raise ValueError(f"Invalid configuration: {exc}") from exc
    _apply_helius_defaults(cfg)
    return cfg


# ---------------------------------------------------------------------------
# Env overrides & normalization
# ---------------------------------------------------------------------------

def apply_env_overrides(config: Mapping[str, Any] | None) -> dict[str, Any]:
    """Merge environment variable overrides into `config` and validate."""
    cfg = dict(config or {})
    for key, env in ENV_VARS.items():
        env_val = os.getenv(env)
        if env_val is not None:
            if isinstance(env_val, str):
                if not env_val.strip():
                    continue
                parsed = None
                try:
                    parsed = loads(env_val)
                except ValueError:
                    try:
                        parsed = ast.literal_eval(env_val)
                    except (ValueError, SyntaxError):
                        logger.debug(
                            "Environment override %s could not be parsed as structured data; using raw string",
                            env,
                        )
                        parsed = None
                if isinstance(parsed, (list, dict)):
                    env_val = parsed
                elif parsed is not None and key == "agent_weights":
                    logger.warning(
                        "Environment override AGENT_WEIGHTS should decode to a mapping; received %s",
                        type(parsed).__name__,
                    )
                elif parsed is None and key == "agent_weights":
                    logger.warning(
                        "Environment override AGENT_WEIGHTS must be JSON or a Python mapping literal",
                    )
            cfg[key] = env_val
    # Validate & normalize via schema
    normalized = validate_config(cfg)
    return normalized


def set_env_from_config(config: dict) -> None:
    """Set environment variables for values present in `config`."""
    for key, env in ENV_VARS.items():
        val = config.get(key)
        if val is None or os.getenv(env) is not None:
            continue
        if isinstance(val, (dict, tuple, set)):
            continue
        if isinstance(val, list):
            if env == "HELIUS_API_KEYS":
                joined = ",".join(str(item).strip() for item in val if str(item).strip())
                if joined:
                    os.environ[env] = joined
            continue
        os.environ[env] = str(val)

    _ensure_helius_environment(config)

    # Ensure SOLANA_WS_URL is always normalized/derived and available
    get_solana_ws_url()

    # Keep shared scanner state in sync with any new environment overrides
    try:
        from . import scanner_common

        scanner_common.refresh_runtime_values()
    except Exception:  # pragma: no cover - optional during early bootstrap
        pass

    # Gentle sanity: warn if someone points JUPITER_WS_URL to a Solana RPC host
    jws = os.getenv("JUPITER_WS_URL", "")
    if jws:
        host = urllib.parse.urlparse(jws).netloc
        if "solana.com" in host or "helius" in host:
            logger.warning(
                "JUPITER_WS_URL looks like a Solana RPC (%s). "
                "If this is unintended, set it to wss://stats.jup.ag/ws",
                host,
            )


def validate_config(cfg: Mapping[str, Any]) -> dict:
    """Validate config against `ConfigModel` schema and return normalized dict."""
    try:
        model = ConfigModel(**cfg)
        return model.model_dump(mode="json") if hasattr(model, "model_dump") else model.dict()
    except ValidationError as exc:
        raise ValueError(f"Invalid configuration: {exc}") from exc


# ---------------------------------------------------------------------------
#  Configuration file management helpers
# ---------------------------------------------------------------------------

_config_dir = os.getenv("CONFIG_DIR", "configs")
if not os.path.isabs(_config_dir):
    CONFIG_DIR = str(ROOT / _config_dir)
else:
    CONFIG_DIR = _config_dir
ACTIVE_CONFIG_FILE = os.path.join(CONFIG_DIR, "active")
os.makedirs(CONFIG_DIR, exist_ok=True)


def find_config_file() -> str | None:
    """Return the first existing configuration file path.

    Search order: env SOLHUNTER_CONFIG, then CWD and package root for:
    config.toml, config.yaml, config.yml.
    """
    path = os.getenv("SOLHUNTER_CONFIG")
    if path:
        p = Path(path)
        if not p.is_absolute():
            cwd_path = Path.cwd() / p
            if cwd_path.is_file():
                return str(cwd_path)
            p = ROOT / p
        if p.is_file():
            return str(p)
    for name in ("config.toml", "config.yaml", "config.yml"):
        p = Path.cwd() / name
        if p.is_file():
            return str(p)
    for name in ("config.toml", "config.yaml", "config.yml"):
        p = ROOT / name
        if p.is_file():
            return str(p)
    return None


def ensure_config_file() -> str | None:
    """Ensure a configuration file exists, generating a default if needed."""
    path = find_config_file()
    if path:
        return path
    try:
        from scripts import quick_setup
        quick_setup.main(["--auto"])
    except Exception:
        logger.exception("Failed to generate default configuration")
        return None
    return find_config_file()


def validate_env(required: Sequence[str], cfg_path: str | None = None) -> dict:
    """
    Ensure required environment variables are set.

    Missing values are filled from the configuration file when possible.
    Returns the loaded configuration dictionary.
    """
    cfg_data: dict[str, Any] = {}
    if cfg_path is None:
        cfg_path = ensure_config_file()
    if cfg_path:
        cfg_data = apply_env_overrides(load_config(cfg_path))
    env_to_key = {v: k for k, v in ENV_VARS.items()}
    missing: list[str] = []
    for name in required:
        if not os.getenv(name):
            val = None
            key = env_to_key.get(name)
            if key:
                val = cfg_data.get(key)
            if val is None:
                val = cfg_data.get(name)
            if val is not None:
                os.environ[name] = str(val)
            if not os.getenv(name):
                missing.append(name)
    if missing:
        for name in missing:
            print(f"Required env var {name} is not set", file=sys.stderr)
        raise SystemExit(1)
    return cfg_data


def list_configs() -> list[str]:
    """Return all saved configuration file names."""
    return [
        f
        for f in os.listdir(CONFIG_DIR)
        if os.path.isfile(os.path.join(CONFIG_DIR, f)) and not f.startswith(".")
    ]


def save_config(name: str, data: bytes) -> None:
    """
    Persist configuration `data` under `name`.

    The name must not contain path traversal components.
    """
    if (
        os.path.sep in name
        or (os.path.altsep and os.path.altsep in name)
        or ".." in name
    ):
        raise ValueError("invalid config name")
    path = os.path.join(CONFIG_DIR, name)
    with open(path, "wb") as fh:
        fh.write(data)
    cfg = {}
    try:
        cfg = _read_config_file(Path(path))
    except Exception:
        logger.exception("Failed to parse configuration file %s", path)
    _publish("config_updated", cfg)


def select_config(name: str) -> None:
    """Mark `name` as the active configuration."""
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    tmp_path = f"{ACTIVE_CONFIG_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        fh.write(name)
    os.replace(tmp_path, ACTIVE_CONFIG_FILE)
    reload_active_config()


def get_active_config_name() -> str | None:
    try:
        with open(ACTIVE_CONFIG_FILE, "r", encoding="utf-8") as fh:
            return fh.read().strip() or None
    except FileNotFoundError:
        return None


def load_selected_config() -> dict:
    """Load the currently selected configuration file."""
    name = get_active_config_name()
    if not name:
        return {}
    path = os.path.join(CONFIG_DIR, name)
    if not os.path.exists(path):
        return {}
    return load_config(path)


def load_dex_config(config: Mapping[str, Any] | None = None) -> DEXConfig:
    """Return `DEXConfig` populated from `config` + env overrides."""
    base_cfg: dict[str, Any] = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY",
        "dex_base_url": "https://swap.helius.dev",
        "dex_partner_urls": {
            "birdeye": "https://api.birdeye.so",
            "jupiter": "https://quote-api.jup.ag",
        },
        "dex_swap_paths": {
            "helius": "/v1/swap",
            "birdeye": "/defi/swap/v1",
            "jupiter": "/v6/swap",
        },
        "dex_priorities": "helius,jupiter,birdeye",
        "agents": ["sim"],
        "agent_weights": {"sim": 1.0},
    }
    if config:
        base_cfg.update(dict(config))
    cfg = apply_env_overrides(base_cfg)

    base = str(cfg.get("dex_base_url", base_cfg["dex_base_url"]))
    testnet = str(cfg.get("dex_testnet_url", base))

    def _url(name: str) -> str:
        return str(cfg.get(name, base))

    def _parse_str_map(val: Any) -> dict[str, str]:
        if not val:
            return {}
        if isinstance(val, Mapping):
            data = val
        elif isinstance(val, str):
            try:
                data = loads(val)
            except Exception:
                logger.exception("Failed to parse JSON mapping: %s", val)
                try:
                    data = ast.literal_eval(val)
                except Exception:
                    logger.exception("Failed to parse literal mapping: %s", val)
                    return {}
        else:
            return {}
        return {str(k): str(v) for k, v in data.items()}

    partner_urls = {
        **base_cfg["dex_partner_urls"],
        **_parse_str_map(cfg.get("dex_partner_urls")),
    }
    swap_paths = {
        **base_cfg["dex_swap_paths"],
        **_parse_str_map(cfg.get("dex_swap_paths")),
    }

    aggregator_urls = {"helius": base, **partner_urls}

    raw_priorities = cfg.get("dex_priorities")
    priorities: list[str]
    if isinstance(raw_priorities, str):
        priorities = [
            p.strip()
            for p in raw_priorities.replace(";", ",").split(",")
            if p.strip()
        ]
    elif isinstance(raw_priorities, Sequence):
        priorities = [str(p).strip() for p in raw_priorities if str(p).strip()]
    else:
        priorities = []

    default_order = [name for name in ("helius", "birdeye", "jupiter") if name in aggregator_urls]
    if not priorities:
        priorities = default_order
    else:
        seen = set()
        normalized: list[str] = []
        for name in priorities:
            if name in aggregator_urls and name not in seen:
                normalized.append(name)
                seen.add(name)
        for name in default_order:
            if name not in seen:
                normalized.append(name)
                seen.add(name)
        for name in aggregator_urls:
            if name not in seen:
                normalized.append(name)
                seen.add(name)
        priorities = normalized

    venue_urls = {
        **aggregator_urls,
        "raydium": _url("raydium_dex_url"),
        "orca": _url("orca_dex_url"),
        "phoenix": _url("phoenix_dex_url"),
        "meteora": _url("meteora_dex_url"),
    }

    def _parse_map(val: Any) -> dict[str, float]:
        if not val:
            return {}
        if isinstance(val, Mapping):
            data = val
        elif isinstance(val, str):
            try:
                data = loads(val)
            except Exception:
                logger.exception("Failed to parse JSON mapping: %s", val)
                try:
                    data = ast.literal_eval(val)
                except Exception:
                    logger.exception("Failed to parse literal mapping: %s", val)
                    return {}
        else:
            return {}
        return {str(k): float(v) for k, v in data.items()}

    fees = _parse_map(cfg.get("dex_fees"))
    gas = _parse_map(cfg.get("dex_gas"))
    latency = _parse_map(cfg.get("dex_latency"))

    return DEXConfig(
        base_url=base,
        testnet_url=testnet,
        venue_urls=venue_urls,
        fees=fees,
        gas=gas,
        latency=latency,
        swap_urls=aggregator_urls,
        swap_paths=swap_paths,
        swap_priorities=priorities,
    )


# ---------------------------------------------------------------------------
#  Active configuration helpers / event bus
# ---------------------------------------------------------------------------

try:
    _ACTIVE_CONFIG: dict[str, Any] = apply_env_overrides(load_selected_config())
except ValueError:
    _ACTIVE_CONFIG = {}
set_env_from_config(_ACTIVE_CONFIG)


def _update_active(cfg: Mapping[str, Any] | None) -> None:
    global _ACTIVE_CONFIG
    if cfg is None:
        cfg = {}
    _ACTIVE_CONFIG = apply_env_overrides(dict(cfg))
    set_env_from_config(_ACTIVE_CONFIG)


def reload_active_config() -> dict:
    """Reload current config and broadcast an update event."""
    cfg = load_selected_config()
    _update_active(cfg)
    _publish("config_updated", cfg)
    return _ACTIVE_CONFIG


def get_event_bus_url(cfg: Mapping[str, Any] | None = None) -> str:
    """
    Return broker URL of the external event bus.

    ``ws://``/``wss://`` URLs keep the legacy websocket bus, while
    ``redis://``/``rediss://`` allows the runtime to connect to Redis-backed
    transports. Falls back to the built-in default when neither env nor config
    provide it.
    """
    if "EVENT_BUS_URL" in _VALIDATED_URLS:
        os.environ.setdefault("EVENT_BUS_URL", _VALIDATED_URLS["EVENT_BUS_URL"])
        return _VALIDATED_URLS["EVENT_BUS_URL"]

    cfg = cfg or _ACTIVE_CONFIG
    url = os.getenv("EVENT_BUS_URL") or str(cfg.get("event_bus_url", ""))
    if url:
        try:
            return _validate_and_store_url(
                "EVENT_BUS_URL", url, {"ws", "wss", "redis", "rediss"}
            )
        except ValueError as exc:
            logger.error(str(exc))

    from .event_bus import DEFAULT_WS_URL
    _VALIDATED_URLS["EVENT_BUS_URL"] = DEFAULT_WS_URL
    os.environ.setdefault("EVENT_BUS_URL", DEFAULT_WS_URL)
    return DEFAULT_WS_URL


def get_event_bus_peers(cfg: Mapping[str, Any] | None = None) -> list[str]:
    """Return list of peer URLs for the event bus."""
    cfg = cfg or _ACTIVE_CONFIG
    env_val = os.getenv("EVENT_BUS_PEERS")
    if env_val:
        peers = [u.strip() for u in env_val.split(",") if u.strip()]
    else:
        raw = cfg.get("event_bus_peers")
        if isinstance(raw, str):
            peers = [u.strip() for u in raw.split(",") if u.strip()]
        elif isinstance(raw, (list, tuple, set)):
            peers = [str(u).strip() for u in raw if str(u).strip()]
        else:
            peers = []
    cleaned: list[str] = []
    seen: set[str] = set()
    for peer in peers:
        parsed = urllib.parse.urlparse(peer)
        scheme = parsed.scheme.lower()
        if scheme not in {"ws", "wss"}:
            logger.warning(
                "Ignoring non-websocket event bus peer %s", _redact(peer)
            )
            continue
        if peer not in seen:
            seen.add(peer)
            cleaned.append(peer)
    return cleaned


def get_broker_url(cfg: Mapping[str, Any] | None = None) -> str | None:
    """Return message broker URL if configured (first of get_broker_urls)."""
    urls = get_broker_urls(cfg)
    return urls[0] if urls else None


def get_broker_urls(cfg: Mapping[str, Any] | None = None) -> list[str]:
    """Return list of message broker URLs (``redis://``, ``rediss://``, ``nats://``)."""
    cfg = cfg or _ACTIVE_CONFIG
    env_val = os.getenv("BROKER_URLS")
    if env_val:
        urls = [u.strip() for u in env_val.split(",") if u.strip()]
    else:
        raw = cfg.get("broker_urls")
        if isinstance(raw, str):
            urls = [u.strip() for u in raw.split(",") if u.strip()]
        elif isinstance(raw, (list, tuple, set)):
            urls = [str(u).strip() for u in raw if str(u).strip()]
        else:
            urls = []
    if not urls:
        url = os.getenv("BROKER_URL") or str(cfg.get("broker_url", ""))
        if url:
            urls = [url]
    allowed_schemes = {"redis", "rediss", "nats"}
    cleaned: list[str] = []
    for url in urls:
        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme in {"ws", "wss"}:
            logger.warning(
                "Ignoring websocket URL %s in BROKER_URLS; use EVENT_BUS_URL or EVENT_BUS_PEERS",
                _redact(url),
            )
            continue
        if not scheme or scheme not in allowed_schemes:
            logger.warning(
                "Ignoring broker URL %s with unsupported scheme %s",
                _redact(url),
                scheme or "(none)",
            )
            continue
        if url not in cleaned:
            cleaned.append(url)
    if not cleaned:
        bus_url = os.getenv("EVENT_BUS_URL", "")
        if bus_url:
            parsed = urllib.parse.urlparse(bus_url)
            if parsed.scheme.lower() in {"redis", "rediss"}:
                cleaned.append(bus_url)
    return cleaned


def get_event_serialization(cfg: Mapping[str, Any] | None = None) -> str | None:
    """Return configured event serialization format."""
    cfg = cfg or _ACTIVE_CONFIG
    val = os.getenv("EVENT_SERIALIZATION") or str(cfg.get("event_serialization", ""))
    return val or None


def get_event_batch_ms(cfg: Mapping[str, Any] | None = None) -> int:
    """Return event batching interval in milliseconds."""
    cfg = cfg or _ACTIVE_CONFIG
    val = os.getenv("EVENT_BATCH_MS")
    if val is None or val == "":
        val = cfg.get("event_batch_ms", 0)
    return int(val or 0)


def get_event_mmap_batch_ms(cfg: Mapping[str, Any] | None = None) -> int:
    """Return mmap event batching interval in milliseconds."""
    cfg = cfg or _ACTIVE_CONFIG
    val = os.getenv("EVENT_MMAP_BATCH_MS")
    if val is None or val == "":
        val = cfg.get("event_mmap_batch_ms", 0)
    return int(val or 0)


def get_event_mmap_batch_size(cfg: Mapping[str, Any] | None = None) -> int:
    """Return number of events to buffer before mmap flush."""
    cfg = cfg or _ACTIVE_CONFIG
    val = os.getenv("EVENT_MMAP_BATCH_SIZE")
    if val is None or val == "":
        val = cfg.get("event_mmap_batch_size", 0)
    return int(val or 0)


def get_depth_ws_addr(cfg: Mapping[str, Any] | None = None) -> tuple[str, int]:
    """Return address and port of the depth websocket server."""
    cfg = cfg or _ACTIVE_CONFIG
    addr = os.getenv("DEPTH_WS_ADDR") or str(cfg.get("depth_ws_addr", "127.0.0.1"))
    port_env = os.getenv("DEPTH_WS_PORT")
    if port_env is not None and port_env != "":
        port = int(port_env)
    else:
        port = int(cast(int, cfg.get("depth_ws_port", 8766)))
    return addr, port


# Subscribe to config updates to keep _ACTIVE_CONFIG in sync
from . import event_bus as _event_bus  # noqa: E402

_sub = _event_bus.subscription("config_updated", _update_active)
_sub.__enter__()


def initialize_event_bus() -> None:
    """Reload event bus settings after environment variables are configured."""
    try:
        _event_bus.initialize_event_bus()
    except Exception:
        logger.exception("Failed to reload event bus settings")
