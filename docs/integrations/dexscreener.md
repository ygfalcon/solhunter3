# DexScreener Integration

## API base URL
- Primary REST base `https://api.dexscreener.com`. Discovery pulls from `/latest/dex/tokens`, `/latest/dex/tokens/trending`, and `/latest/dex/tokens/new`; momentum enrichment also queries `/tokens/v1/solana/{mint}` for route metadata.【F:solhunter_zero/token_scanner.py†L888-L1336】【F:solhunter_zero/golden_pipeline/momentum.py†L1409-L1476】

## Expected JSON payload
- Trending responses return `pairs` or `tokens` arrays with per-entry `priceUsd`, `volume24h`, `liquidity`, `rank`, and `score` fields used to populate discovery metadata and momentum breakdowns.【F:solhunter_zero/token_scanner.py†L895-L1080】【F:solhunter_zero/golden_pipeline/momentum.py†L910-L989】
- Token lookups under `/tokens/v1/solana/{mint}` include route data, on-chain addresses, and volume metrics that augment liquidity heuristics.【F:solhunter_zero/golden_pipeline/momentum.py†L1444-L1476】
- Price fallback hits `/latest/dex/tokens?pairAddress=<mint>` returning aggregated swap data consumed by the arbitrage module.【F:solhunter_zero/arbitrage.py†L1030-L1130】

## Retry/backoff rules
- Host guard rails allow 8 concurrent requests with a 0.3s base backoff and 20s cooldown when 3 failures occur. Momentum agent additionally throttles the host at 5 rps with burst 5 via token buckets.【F:solhunter_zero/http.py†L264-L306】【F:solhunter_zero/golden_pipeline/momentum.py†L60-L83】
- Discovery scheduler fans out tasks but records breaker state to drop failing hosts after repeated errors.【F:solhunter_zero/token_scanner.py†L1285-L1370】

## Schema sample
```http
GET /latest/dex/tokens/trending
Accept: application/json
```
```json
{
  "pairs": [
    {
      "address": "8HoQnePLqPj4M7Pz5DooujkGfag9nJwQWd1xQDdCTjMm",
      "baseToken": { "address": "So111...", "symbol": "SOL" },
      "priceUsd": "173.52",
      "volume24h": "12500000",
      "fdv": "80000000",
      "rank": 3,
      "score": 0.92
    }
  ]
}
```

## Testing endpoint command
```bash
curl -s "${DEXSCREENER_API_URL:-https://api.dexscreener.com/latest/dex/tokens/trending}"
```
