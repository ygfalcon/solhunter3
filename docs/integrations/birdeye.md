# Birdeye Integration

## API base URL
- REST base defaults to `https://api.birdeye.so`, while momentum enrichment uses the public mirror `https://public-api.birdeye.so`. Both accept overrides via `BIRDEYE_BASE`, `BIRDEYE_API_KEY`, and `BIRDEYE_API`.【F:solhunter_zero/onchain_metrics.py†L71-L116】【F:solhunter_zero/golden_pipeline/momentum.py†L60-L83】【F:solhunter_zero/token_discovery.py†L118-L216】

## Expected JSON payload
- Price/overview calls hit `/defi/price`, `/defi/token_overview`, and `/defi/ohlcv` with `address=<mint>` plus `chain=solana`, returning price, volume, liquidity, and OHLCV arrays that feed market metrics and discovery.【F:solhunter_zero/onchain_metrics.py†L223-L284】
- Trending snapshots read `/defi/token_trending` and `/defi/v3/token/list`, yielding per-mint `score`, `rank`, `volume1h/24h`, and buyer counts used in momentum scoring.【F:solhunter_zero/golden_pipeline/momentum.py†L1289-L1397】
- Token discovery hydrates candidate metadata from `/defi/tokenlist` and `/defi/token_overview`, merging venues and symbol data.【F:solhunter_zero/token_discovery.py†L580-L835】

## Retry/backoff rules
- Host guard rails: `public-api.birdeye.so` limited to 6 concurrent requests, threshold 3 failures, 30s cooldown, 0.35s base backoff; mirrored settings apply via `_HOST_RPS` (5 rps, burst 5).【F:solhunter_zero/http.py†L264-L306】【F:solhunter_zero/golden_pipeline/momentum.py†L60-L83】
- API key resolution enforces fingerprint logging and cooldown resets when 401/403 responses occur; metrics reuse cached semaphores for concurrency control.【F:solhunter_zero/prices.py†L481-L563】【F:solhunter_zero/onchain_metrics.py†L52-L94】

## Schema sample
```http
GET /defi/price?address=So11111111111111111111111111111111111111112&chain=solana
x-api-key: <BIRDEYE_API_KEY>
```
```json
{
  "success": true,
  "data": {
    "value": 173.52,
    "updateUnixTime": 1716509123,
    "liquidity": 8200000,
    "volume24hUSD": 12500000
  }
}
```

## Testing endpoint command
```bash
curl -s "${BIRDEYE_PRICE_URL:-https://api.birdeye.so}/defi/price?address=So11111111111111111111111111111111111111112&chain=solana" -H "x-api-key: ${BIRDEYE_API_KEY:?set birdeye key}"
```
