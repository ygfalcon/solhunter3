# LunarCrush Integration

## API base URL
- Momentum fallback polls `https://lunarcrush.com/api4/public/coins/{symbol}` by default; override via `MOMENTUM_LUNARCRUSH_URL_TEMPLATE` or config YAML entries. Host name is extracted to seed rate limits and circuit breakers.【F:solhunter_zero/golden_pipeline/momentum.py†L259-L333】【F:solhunter_zero/golden_pipeline/momentum.py†L604-L648】

## Expected JSON payload
- Responses may be a list or an object with `data` containing per-coin metrics. The agent looks for `social_sentiment`, `tweet_volume`/`tweets_last_24h`, `galaxy_score`, and `social_score`, normalising values into 0–1 sentiment scores and tweets per minute. Payloads also supply a `social_source` tag propagated to UI badges.【F:solhunter_zero/golden_pipeline/momentum.py†L1632-L1705】【F:solhunter_zero/golden_pipeline/momentum.py†L1576-L1620】

## Retry/backoff rules
- LunarCrush calls are optional; they are enabled only when both the rate-free mode and `enable_lunarcrush_fallback` flag are true. Requests share the host limiter map (default 1 rps, burst 3) and circuit breaker that records failures and opens after repeated errors. Results are cached for `_LUNARCRUSH_CACHE_TTL_SEC` (60s) with delta tracking to highlight sentiment changes.【F:solhunter_zero/golden_pipeline/momentum.py†L60-L116】【F:solhunter_zero/golden_pipeline/momentum.py†L583-L649】【F:solhunter_zero/golden_pipeline/momentum.py†L1632-L1705】【F:solhunter_zero/golden_pipeline/momentum.py†L2026-L2034】

## Schema sample
```http
GET /api4/public/coins/SOL
Accept: application/json
```
```json
{
  "data": [
    {
      "symbol": "SOL",
      "social_sentiment": 0.74,
      "tweet_volume": 9213,
      "galaxy_score": 67.2,
      "social_score": 53123
    }
  ]
}
```

## Testing endpoint command
```bash
curl -s "https://lunarcrush.com/api4/public/coins/SOL"${LUNARCRUSH_API_KEY:+"?key=$LUNARCRUSH_API_KEY"}
```
