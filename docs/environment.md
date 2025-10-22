# Environment Variables

This document lists environment variables recognized by the project.

## Production environment precedence

The `load_production_env` helper reads candidate production environment files in
priority order. Values defined in an earlier file take precedence over later
files unless the loader is invoked with `overwrite=True`.

| Variable | Default | Description |
|---|---|---|
| `ACTIVITY_MODEL_PATH` | `` | Path to activity model |
| `AGENTS` | `` | Configures agents |
| `AGENT_WEIGHTS` | `` | Configures agent weights |
| `ARBITRAGE_AMOUNT` | `0` | Configures arbitrage amount |
| `ARBITRAGE_THRESHOLD` | `0` | Threshold for arbitrage |
| `ARBITRAGE_TOKENS` | `` | Configures arbitrage tokens |
| `AUTO_SELECT_KEYPAIR` | `` | Configures auto select keypair |
| `BIRDEYE_API_KEY` | `b1e60d72780940d1bd929b9b2e9225e6` | Configures birdeye api key |
| `BROKER_CHANNEL` | `solhunter-events-v3` | Configures broker channel |
| `BROKER_HEARTBEAT_INTERVAL` | `30` | Interval for broker heartbeat |
| `BROKER_RETRY_LIMIT` | `3` | Configures broker retry limit |
| `BROKER_URL` | `` | URL for broker |
| `BROKER_URLS` | `` | Configures broker urls |
| `BROKER_VERIFY_ABORT` | `` | Configures broker verify abort |
| `BUNDLE_SIZE` | `1` | Configures bundle size |
| `COLLECT_OFFLINE_DATA` | `false` | Configures collect offline data |
| `COMPRESS_EVENTS` | `` | Configures compress events |
| `CONCURRENCY_EWM_SMOOTHING` | `0.15` | Configures concurrency ewm smoothing |
| `CONCURRENCY_KI` | `0.0` | Configures concurrency ki |
| `CONCURRENCY_KP` | `0.5` | Configures concurrency kp |
| `CONCURRENCY_SMOOTHING` | `` | Configures concurrency smoothing |
| `CONFIG_DIR` | `configs` | Configures config dir |
| `CPU_HIGH_THRESHOLD` | `80` | Threshold for cpu high |
| `CPU_LOW_THRESHOLD` | `40` | Threshold for cpu low |
| `DEPTH_CACHE_TTL` | `0.5` | TTL for depth cache cache |
| `DEPTH_MAX_RESTARTS` | `` | Configures depth max restarts |
| `DEPTH_MMAP_PATH` | `/tmp/depth_service.mmap` | Path to depth mmap |
| `DEPTH_MMAP_POLL_INTERVAL` | `1` | Interval for depth mmap poll |
| `DEPTH_SERVICE_SOCKET` | `/tmp/depth_service.sock` | Configures depth service socket |
| `DEPTH_START_TIMEOUT` | `10` | Configures depth start timeout |
| `DEPTH_WS_ADDR` | `127.0.0.1` | Address for depth ws |
| `DEPTH_WS_PORT` | `8766` | Port for depth ws |
| `DEX_LATENCY_CACHE_TTL` | `30` | TTL for dex latency cache cache |
| `DEX_LATENCY_REFRESH_INTERVAL` | `60` | Interval for dex latency refresh |
| `DEX_LISTING_WS_URL` | `` | URL for dex listing ws |
| `DEX_METRICS_CACHE_TTL` | `30` | TTL for dex metrics cache cache |
| `DEX_METRIC_URLS` | `` | Configures dex metric urls |
| `DISCORD_FEEDS` | `https://discord.com/api/guilds/613425648685547541/widget.json` | Configures discord feeds |
| `DISCOVERY_METHOD` | `websocket` | Configures discovery method |
| `DISCOVERY_SOCIAL_LIMIT` | `12` | Maximum number of social tokens to merge into discovery |
| `DISCOVERY_SOCIAL_MIN_MENTIONS` | `2` | Minimum mention count required for social candidates |
| `DISCOVERY_SOCIAL_SAMPLE_LIMIT` | `3` | Maximum example snippets stored per social token |
| `DYNAMIC_CONCURRENCY_INTERVAL` | `` | Interval for dynamic concurrency |
| `EDGE_CACHE_TTL` | `60` | TTL for edge cache cache |
| `EVENT_BATCH_MS` | `` | Configures event batch ms |
| `EVENT_BUS_COMPRESSION` | `` | Configures event bus compression |
| `EVENT_BUS_MMAP` | `` | Configures event bus mmap |
| `EVENT_BUS_MMAP_SIZE` | `` | Configures event bus mmap size |
| `EVENT_BUS_PEERS` | `` | Configures event bus peers |
| `EVENT_BUS_URL` | `ws://0.0.0.0:8787` | URL for event bus |
| `EVENT_COMPRESSION` | `` | Configures event compression |
| `EVENT_COMPRESSION_THRESHOLD` | `512` | Threshold for event compression |
| `EVENT_MMAP_BATCH_MS` | `` | Configures event mmap batch ms |
| `EVENT_MMAP_BATCH_SIZE` | `` | Configures event mmap batch size |
| `EVENT_SERIALIZATION` | `` | Configures event serialization |
| `EVENT_WS_PORT` | `8770` | Port for event websocket |
| `EXPORT_ONNX` | `` | Configures export onnx |
| `FIRST_TRADE_RETRY` | `false` | Configures first trade retry |
| `FIRST_TRADE_TIMEOUT` | `0` | Configures first trade timeout |
| `FLASH_LOAN_RATIO` | `0` | Configures flash loan ratio |
| `FORCE_CPU_INDEX` | `` | Configures force cpu index |
| `GAS_MULTIPLIER` | `1.0` | Configures gas multiplier |
| `GNN_MODEL_PATH` | `route_gnn.pt` | Path to gnn model |
| `GPU_MEMORY_INDEX` | `` | Configures gpu memory index |
| `GRAPH_MODEL_PATH` | `` | Path to graph model |
| `HELIUS_API_KEY` | `YOUR_HELIUS_KEY` | API key used for Helius price and asset requests |
| `HELIUS_PRICE_REST_URL` | `` | Optional legacy REST price endpoint for Helius |
| `HELIUS_PRICE_RPC_METHOD` | `getAssetBatch` | JSON-RPC method used when querying Helius prices |
| `HELIUS_PRICE_RPC_URL` | `https://rpc.helius.xyz` | Helius RPC endpoint for token price lookups |
| `HTTP_CONNECTOR_LIMIT` | `0` | Configures http connector limit |
| `HTTP_CONNECTOR_LIMIT_PER_HOST` | `0` | Configures http connector limit per host |
| `JITO_AUTH` | `` | Authentication token for Jito; obtained by signing the block engine challenge with the Solhunter trading keypair that Jito has whitelisted and stored in `.env`; override via environment variable or personal config |
| `JITO_RPC_URL` | `` | URL for jito rpc |
| `JITO_WS_AUTH` | `` | Configures jito ws auth |
| `JITO_WS_URL` | `` | URL for jito ws |
| `JUPITER_API_URL` | `https://price.jup.ag/v4/price` | URL for jupiter api |
| `JUPITER_WS_URL` | `wss://stats.jup.ag/ws` | URL for jupiter ws |
| `KEYPAIR_DIR` | `keypairs` | Configures keypair dir |
| `KEYPAIR_JSON` | `` | Configures keypair json |
| `KEYPAIR_PATH` | `` | Path to keypair |
| `LISTING_CACHE_TTL` | `60` | TTL for listing cache cache |
| `LOG_LEVEL` | `` | Configures log level |
| `MARKET_WS_URL` | `` | URL for market ws |
| `MAX_ALLOCATION` | `0.2` | Maximum allocation |
| `MAX_DRAWDOWN` | `1.0` | Maximum drawdown |
| `MAX_FLASH_AMOUNT` | `0` | Maximum flash amount |
| `MAX_HOPS` | `3` | Maximum hops |
| `MAX_RISK_PER_TOKEN` | `0.1` | Maximum risk per token |
| `MEASURE_DEX_LATENCY` | `1` | Configures measure dex latency |
| `MEMORY_BATCH_SIZE` | `` | Configures memory batch size |
| `MEMORY_FLUSH_INTERVAL` | `` | Interval for memory flush |
| `MEMORY_SYNC_INTERVAL` | `` | Interval for memory sync |
| `MEMPOOL_SCORE_THRESHOLD` | `0` | Threshold for mempool score |
| `MEMPOOL_STATS_WINDOW` | `5` | Configures mempool stats window |
| `MEMPOOL_THRESHOLD` | `0` | Threshold for mempool |
| `MEMPOOL_WEIGHT` | `0.0001` | Configures mempool weight |
| `METEORA_API_URL` | `https://api.meteora.ag` | URL for meteora api |
| `METEORA_DEPTH_WS_URL` | `` | URL for meteora depth ws |
| `METEORA_WS_URL` | `` | URL for meteora ws |
| `METRICS_BASE_URL` | `https://api.coingecko.com/api/v3` | URL for metrics base |
| `METRICS_URL` | `` | URL for metrics |
| `MIN_PORTFOLIO_VALUE` | `20` | Minimum portfolio value |
| `MIN_STARTING_BALANCE` | `0` | Minimum starting balance |
| `MNEMONIC` | `` | Configures mnemonic |
| `ENCRYPT_MNEMONIC` | `0` | Store mnemonic file encrypted when set to `1` |
| `MNEMONIC_ENCRYPTION_KEY` | `` | Passphrase used for mnemonic encryption |
| `NEWS_FEEDS` | `https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml,https://www.coindesk.com/arc/outboundfeeds/rss/` | Configures news feeds |
| `OFFLINE_BATCH_SIZE` | `` | Configures offline batch size |
| `OFFLINE_DATA_INTERVAL` | `3600` | Interval for offline data |
| `OFFLINE_DATA_LIMIT_GB` | `` | Configures offline data limit gb |
| `OFFLINE_FLUSH_INTERVAL` | `` | Interval for offline flush |
| `OFFLINE_FLUSH_MAX_BATCH` | `` | Configures offline flush max batch |
| `OFFLINE_MEMMAP_PATH` | `` | Path to offline memmap |
| `OFFLINE_MEMMAP_SIZE` | `` | Configures offline memmap size |
| `ONCHAIN_MODEL_PATH` | `` | Path to onchain model |
| `ORCA_API_URL` | `https://api.orca.so` | URL for orca api |
| `ORCA_WS_URL` | `` | URL for orca ws |
| `ORDERBOOK_CACHE_TTL` | `5` | TTL for orderbook cache cache |
| `ORDER_BOOK_WS_URL` | `` | URL for order book ws |
| `PASSPHRASE` | `` | Configures passphrase |
| `PATH_ALGORITHM` | `graph` | Configures path algorithm |
| `PHOENIX_API_URL` | `https://api.phoenix.trade` | URL for phoenix api |
| `PHOENIX_DEPTH_WS_URL` | `` | URL for phoenix depth ws |
| `PHOENIX_WS_URL` | `` | URL for phoenix ws |
| `PORTFOLIO_VALUE` | `0` | Configures portfolio value |
| `PRICES_MAX_AGE_MONTHS` | `6` | Configures prices max age months |
| `PRICE_API_URL` | `https://price.jup.ag` | URL for price api |
| `PRICE_CACHE_TTL` | `30` | TTL for price cache cache |
| `PRICE_MODEL_PATH` | `` | Path to price model |
| `PYTORCH_ENABLE_MPS_FALLBACK` | `` | Configures pytorch enable mps fallback |
| `RAISE_ON_WS_FAIL` | `` | Configures raise on ws fail |
| `RAYDIUM_API_URL` | `https://api.raydium.io` | URL for raydium api |
| `RAYDIUM_WS_URL` | `` | URL for raydium ws |
| `RECENT_TRADE_WINDOW` | `0` | Configures recent trade window |
| `REGIME_MODEL_PATH` | `` | Path to regime model |
| `RISK_MULTIPLIER` | `1.0` | Configures risk multiplier |
| `RISK_TOLERANCE` | `0.1` | Configures risk tolerance |
| `RL_BUILD_MMAP_DATASET` | `1` | Reinforcement learning setting for build mmap dataset |
| `RL_CLUSTER_WORKERS` | `1` | Reinforcement learning setting for cluster workers |
| `RL_DYNAMIC_WORKERS` | `` | Reinforcement learning setting for dynamic workers |
| `RL_MAX_INTERVAL` | `` | Interval for rl max |
| `RL_MIN_INTERVAL` | `` | Interval for rl min |
| `RL_NUM_WORKERS` | `` | Reinforcement learning setting for num workers |
| `RL_POLICY_PATH` | `rl_policy.json` | Path to rl policy |
| `RL_PREFETCH_BUFFER` | `` | Reinforcement learning setting for prefetch buffer |
| `RL_PRIORITIZED_REPLAY` | `` | Reinforcement learning setting for prioritized replay |
| `RL_WORKERS` | `` | Reinforcement learning setting for workers |
| `ROUTE_FFI_LIB` | `` | Configures route ffi lib |
| `ROUTE_GENERATOR_PATH` | `route_generator.pt` | Path to route generator |
| `SHELL` | `` | Configures shell |
| `SIM_MODEL_CACHE_TTL` | `300` | TTL for sim model cache cache |
| `SOLANA_KEYPAIR` | `` | Configures solana keypair |
| `SOLANA_RPC_URL` | `https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY` | URL for Solana RPC and default endpoint for connectivity checks |
| `SOLANA_TESTNET_RPC_URL` | `https://devnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY` | URL for solana testnet rpc |
| `SOLHUNTER_CONFIG` | `` | Configures solhunter config |
| `SOLHUNTER_FORCE_DEPS` | `` | Configures solhunter force deps |
| `SOLHUNTER_GPU_AVAILABLE` | `` | Configures solhunter gpu available |
| `SOLHUNTER_GPU_DEVICE` | `` | Configures solhunter gpu device |
| `SOLHUNTER_INSTALL_OPTIONAL` | `` | Configures solhunter install optional |
| `SOLHUNTER_NO_DIAGNOSTICS` | `` | Configures solhunter no diagnostics |
| `SOLHUNTER_SKIP_DEPS` | `` | Configures solhunter skip deps |
| `SOLHUNTER_SKIP_PREFLIGHT` | `` | Configures solhunter skip preflight |
| `SOLHUNTER_SKIP_SETUP` | `` | Configures solhunter skip setup |
| `SOLHUNTER_SKIP_VENV` | `` | Configures solhunter skip venv |
| `SOLHUNTER_FAST` | `` | Skip repeated environment checks when markers exist |
| `SOLHUNTER_TESTING` | `` | Configures solhunter testing |
| `STOP_LOSS` | `0` | Configures stop loss |
| `STRATEGIES` | `` | Configures strategies |
| `TAKE_PROFIT` | `0` | Configures take profit |
| `TOKEN_BLACKLIST` | `` | Configures token blacklist |
| `TOKEN_DISCOVERY_BACKOFF` | `1` | Configures token discovery backoff |
| `TOKEN_KEYWORDS` | `` | Configures token keywords |
| `TOKEN_METRICS_CACHE_TTL` | `30` | TTL for token metrics cache cache |
| `TOKEN_SUFFIX` | `bonk` | Configures token suffix |
| `TOKEN_VOLUME_CACHE_TTL` | `30` | TTL for token volume cache cache |
| `TOP_VOLUME_TOKENS_CACHE_TTL` | `60` | TTL for top volume tokens cache cache |
| `TORCHVISION_METAL_VERSION` | `` | Configures torchvision metal version |
| `TORCH_METAL_VERSION` | `` | Configures torch metal version |
| `TRAILING_STOP` | `0` | Configures trailing stop |
| `TREND_CACHE_TTL` | `60` | TTL for trend cache cache |
| `TWITTER_FEEDS` | `https://nitter.net/solana/rss` | Configures twitter feeds |
| `USE_DEPTH_FEED` | `0` | Enable depth feed |
| `USE_DEPTH_STREAM` | `1` | Enable depth stream |
| `USE_FFI_ROUTE` | `` | Enable ffi route |
| `USE_FLASH_LOANS` | `0` | Enable flash loans |
| `USE_GNN_ROUTING` | `0` | Enable gnn routing |
| `USE_GPU_SIM` | `` | Enable gpu sim |
| `USE_MEV_BUNDLES` | `false` | Enable mev bundles |
| `USE_NUMBA_ROUTE` | `0` | Enable numba route |
| `USE_PRICE_STREAMS` | `0` | Enable price streams |
| `USE_RUST_EXEC` | `True` | Enable rust exec |
| `USE_SERVICE_EXEC` | `True` | Enable service exec |
| `USE_SERVICE_ROUTE` | `1` | Enable service route |
| `USE_TORCH_COMPILE` | `1` | Enable torch compile |
| `USE_ZLIB_EVENTS` | `` | Enable zlib events |
| `VAR_CONFIDENCE` | `0.95` | Configures var confidence |
| `VAR_MODEL_PATH` | `` | Path to var model |
| `VAR_THRESHOLD` | `0` | Threshold for var |
| `VAR_WINDOW` | `30` | Configures var window |
| `VOLATILITY_FACTOR` | `1.0` | Configures volatility factor |
| `VOLUME_THRESHOLD` | `0` | Threshold for volume |
| `WS_PING_INTERVAL` | `20` | Interval for ws ping |
| `WS_PING_TIMEOUT` | `20` | Configures ws ping timeout |

## PyTorch Metal versions

When running on Apple Silicon the Metal builds of ``torch`` and
``torchvision`` must be installed as a matching pair. Supported combinations
are:

| torch | torchvision |
| ----- | ----------- |
| 2.8.0 | 0.23.0 |

Set the ``TORCH_METAL_VERSION`` and ``TORCHVISION_METAL_VERSION`` environment
variables or edit the ``[torch]`` section in ``config.toml`` to override the
defaults.

## Jito Authentication

On first launch the project checks for `JITO_AUTH`. The automated token
minting flow is no longer available—Jito is not issuing new
block-engine credentials via the public challenge endpoint—so the
launcher can only reuse a token that already exists in `.env`. To
provide or replace the cached value, set `JITO_AUTH` in the environment
or edit `.env` before starting the application.
