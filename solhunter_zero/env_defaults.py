"""Default environment variable values for SolHunter Zero.

This module centralizes the default values for environment variables used
throughout the project. :func:`solhunter_zero.env_config.configure_environment`
loads these defaults if the variables are not already defined in the host
environment.
"""

from __future__ import annotations

DEFAULTS: dict[str, str] = {
    "BIRDEYE_API_KEY": "b1e60d72780940d1bd929b9b2e9225e6",
    # Core service toggles
    "DEPTH_SERVICE": "true",
    "USE_DEPTH_STREAM": "1",
    "USE_DEPTH_FEED": "0",
    "USE_RUST_EXEC": "True",
    "USE_SERVICE_EXEC": "True",

    # Solana RPC endpoints
    "SOLANA_RPC_URL": "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    "SOLANA_TESTNET_RPC_URL": "https://devnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    "DEX_BASE_URL": "https://swap.helius.dev",
    "AGENTS": "['sim']",
    "AGENT_WEIGHTS": "{'sim': 1.0}",

    # Depth service paths and settings
    "DEPTH_SERVICE_SOCKET": "/tmp/depth_service.sock",
    "DEPTH_MMAP_PATH": "/tmp/depth_service.mmap",
    "DEPTH_MMAP_POLL_INTERVAL": "1",
    "DEPTH_START_TIMEOUT": "10",
    "DEPTH_MAX_RESTARTS": "1",
    "DEPTH_CACHE_TTL": "0.5",

    # DEX and API endpoints
    "ORCA_API_URL": "https://api.orca.so",
    "RAYDIUM_API_URL": "https://api.raydium.io",
    "PHOENIX_API_URL": "https://app.phoenix.trade",
    "METEORA_API_URL": "https://app.meteora.ag",
    "ORCA_WS_URL": "",
    "RAYDIUM_WS_URL": "",
    "PHOENIX_WS_URL": "",
    "METEORA_WS_URL": "",
    "JUPITER_WS_URL": "wss://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    "ORCA_DEX_URL": "https://www.orca.so",
    "RAYDIUM_DEX_URL": "https://raydium.io",
    "PHOENIX_DEX_URL": "https://app.phoenix.trade",
    "METEORA_DEX_URL": "https://app.meteora.ag",
    "DEX_LISTING_WS_URL": "",

    # Scanning defaults
    "TOKEN_SUFFIX": "",
    "TOKEN_KEYWORDS": "",
    "TOKEN_EXCLUDE_KEYWORDS": "scam,rug",
    "DISCOVERY_MIN_VOLUME_USD": "50000",
    "DISCOVERY_MIN_LIQUIDITY_USD": "75000",
    "DISCOVERY_MAX_TOKENS": "50",
    "DISCOVERY_OVERFETCH_FACTOR": "1.6",
    "DISCOVERY_CACHE_TTL": "45",
    "DISCOVERY_MEMPOOL_LIMIT": "12",
    "DISCOVERY_VOLUME_WEIGHT": "0.45",
    "DISCOVERY_LIQUIDITY_WEIGHT": "0.55",
    "DISCOVERY_MEMPOOL_BONUS": "5.0",
    "VOLUME_THRESHOLD": "0",
    "TREND_CACHE_TTL": "60",
    "LISTING_CACHE_TTL": "60",
    "TOKEN_METRICS_CACHE_TTL": "30",
    "SIM_MODEL_CACHE_TTL": "300",
    "DEX_METRIC_URLS": "",
    "MEMPOOL_STATS_WINDOW": "5",
    "MEMPOOL_SCORE_THRESHOLD": "0",
    "TOKEN_BLACKLIST": "",

    # Networking
    "HTTP_CONNECTOR_LIMIT": "0",
    "HTTP_CONNECTOR_LIMIT_PER_HOST": "0",
    "WS_PING_INTERVAL": "20",
    "WS_PING_TIMEOUT": "20",

    # Event bus
    "EVENT_COMPRESSION_THRESHOLD": "512",
    "EVENT_MMAP_BATCH_MS": "5",
    "EVENT_MMAP_BATCH_SIZE": "16",
    "EVENT_BATCH_MS": "10",
    "BROKER_CHANNEL": "solhunter-events",

    # Order book service
    "ORDERBOOK_CACHE_TTL": "5",

    # Concurrency control
    "CONCURRENCY_SMOOTHING": "0.5",
    "CONCURRENCY_KP": "0.5",
    "CONCURRENCY_KI": "0.0",
    "CONCURRENCY_EWM_SMOOTHING": "0.15",
    "CPU_HIGH_THRESHOLD": "80",
    "CPU_LOW_THRESHOLD": "40",

    # Risk parameters
    "RISK_TOLERANCE": "0.1",
    "MAX_ALLOCATION": "0.2",
    "MAX_RISK_PER_TOKEN": "0.1",
    "RISK_MULTIPLIER": "1.0",
    "MIN_PORTFOLIO_VALUE": "20",
    "VAR_CONFIDENCE": "0.95",
    "VAR_WINDOW": "30",
    "VAR_THRESHOLD": "0",
    "ARBITRAGE_THRESHOLD": "0",
    "ARBITRAGE_AMOUNT": "0",
    "USE_MEV_BUNDLES": "false",

    # Miscellaneous
    "RL_POLICY_PATH": "rl_policy.json",
    "OFFLINE_DATA_INTERVAL": "3600",
    "DEPTH_WS_ADDR": "127.0.0.1",
    "DEPTH_WS_PORT": "8766",
    "EVENT_WS_PORT": "8770",
    "KEYPAIR_DIR": "keypairs",
    "RL_CLUSTER_WORKERS": "1",
    "RL_BUILD_MMAP_DATASET": "1",
    "NEWS_FEEDS": "",
    "TWITTER_FEEDS": "",
    "DISCORD_FEEDS": "",
    "USE_TORCH_COMPILE": "1",
    "FORCE_CPU_INDEX": "",
    "PYTORCH_ENABLE_MPS_FALLBACK": "1",
    "COLLECT_OFFLINE_DATA": "false",
}
