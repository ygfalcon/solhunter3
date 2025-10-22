# Pump.fun Integration

## API base URL
- Uses the public Pump.fun leaderboard at `https://pump.fun/api/trending`. The URL is configurable via `PUMP_FUN_TRENDING` or `PUMP_LEADERBOARD_URL` for discovery fallbacks.【F:solhunter_zero/golden_pipeline/momentum.py†L60-L108】【F:solhunter_zero/token_scanner.py†L175-L261】

## Expected JSON payload
- Responses provide a `tokens` array where each entry includes `tokenAddress`, `rank`, `score`/`pumpScore`, `buyersLastHour`, `tweetsLastHour`, and optional `sentiment`. These values feed the momentum calculator (`pump_intensity`, `pump_score`, buyer counts) and discovery metadata sources.【F:solhunter_zero/golden_pipeline/momentum.py†L1703-L1763】【F:solhunter_zero/token_scanner.py†L822-L1374】
- LunarCrush fallback merges additional sentiment into the same breakdown when Pump.fun coverage is partial.【F:solhunter_zero/golden_pipeline/momentum.py†L1154-L1186】

## Retry/backoff rules
- Momentum agent enforces a token-bucket limiter at 3 rps with burst 3 for `pump.fun`, plus host-level circuit breakers that back off for 60 seconds after repeated failures. Cached responses persist for `_SOCIAL_CACHE_TTL_SEC` (5 minutes).【F:solhunter_zero/golden_pipeline/momentum.py†L60-L116】【F:solhunter_zero/golden_pipeline/momentum.py†L749-L821】【F:solhunter_zero/golden_pipeline/momentum.py†L1719-L1744】
- Discovery fallbacks log provider errors and short-circuit when Pump.fun data is unavailable to avoid blocking DAS flows.【F:solhunter_zero/token_scanner.py†L1237-L1374】

## Schema sample
```http
GET /api/trending
Accept: application/json
```
```json
{
  "tokens": [
    {
      "tokenAddress": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
      "rank": 2,
      "score": 0.91,
      "buyersLastHour": 182,
      "tweetsLastHour": 64,
      "sentiment": 0.74
    }
  ]
}
```

## Testing endpoint command
```bash
curl -s "${PUMP_LEADERBOARD_URL:-https://pump.fun/api/trending}"
```
