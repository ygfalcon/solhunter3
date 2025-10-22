# Meteora Integration

## API base URL
- Core REST base `https://api.meteora.ag` (overridable via `METEORA_API_URL`) for token price lookups at `/price?token=<mint>`. Pool discovery uses `https://dlmm-api.meteora.ag/api/pools/latest` controlled by `METEORA_POOLS_URL` and feature flags.【F:solhunter_zero/arbitrage.py†L289-L336】【F:solhunter_zero/token_discovery.py†L87-L1080】

## Expected JSON payload
- Price endpoint replies with `{ "price": <float> }`, which arbitrage caches and feeds back into consolidated price books.【F:solhunter_zero/arbitrage.py†L998-L1028】
- Pool listings return arrays (or wrapped objects) containing per-pool `tokenMint`, `liquidity`, `volume24h`, `priceUsd`, and timestamps; discovery normalises these into candidate entries with `symbol`, `name`, `pool_address`, and liquidity heuristics.【F:solhunter_zero/token_discovery.py†L954-L1044】

## Retry/backoff rules
- Host guard rails enforce a 4-connection semaphore with 0.3s base backoff and 20s cooldown after three consecutive failures. Momentum agent mirrors this with a 5 rps bucket for `api.meteora.ag`.【F:solhunter_zero/http.py†L264-L306】【F:solhunter_zero/golden_pipeline/momentum.py†L60-L83】
- Discovery helper caches ETag responses and retries per `host_retry_config`, backing off exponentially across attempts.【F:solhunter_zero/token_discovery.py†L520-L612】

## Schema sample
```http
GET /price?token=So11111111111111111111111111111111111111112
Accept: application/json
```
```json
{
  "price": 173.52
}
```

## Testing endpoint command
```bash
curl -s "${METEORA_API_URL:-https://api.meteora.ag}/price?token=So11111111111111111111111111111111111111112"
```
