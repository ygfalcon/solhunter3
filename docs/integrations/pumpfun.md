# Pump.fun Integration

## API coverage and precedence
- Momentum ranking still calls the native Pump.fun trending feed at `https://pump.fun/api/trending` and normalises both the legacy Pump.fun keys and the PumpPortal aliases (`tokenAddress`/`address`, `score`/`pumpScore`, etc.).【F:solhunter_zero/golden_pipeline/momentum.py†L1718-L1740】
- Discovery fallbacks query PumpPortal by default (`https://pumpportal.fun/api/leaderboard`), accepting either the PumpPortal schema (`mint`, `image_url`, …) or the original Pump.fun fields (`tokenMint`, `imageUrl`, …).【F:solhunter_zero/token_scanner.py†L1693-L1744】
- Set `PUMP_LEADERBOARD_URL` to override the discovery endpoint; the legacy `PUMP_FUN_TRENDING` knob only affects the momentum fetcher, so discovery continues to prefer the leaderboard URL when both are configured.【F:solhunter_zero/token_scanner.py†L1693-L1744】【F:solhunter_zero/golden_pipeline/momentum.py†L1718-L1740】

## Leaderboard query parameters
- `PUMP_LEADERBOARD_SORT` (default `volume_24h`) and `PUMP_LEADERBOARD_TIMEFRAME` (default `24h`) are forwarded as query parameters alongside the requested limit when calling the leaderboard URL.【F:solhunter_zero/token_scanner.py†L1693-L1705】

## Discovery metadata propagation
- Each leaderboard entry is stored under `TRENDING_METADATA[mint]` with `source="pumpfun"`, retaining any provided name, symbol, and icon, and promoting PumpPortal analytics (`liquidity`, `volume_24h`, `volume_1h`, `volume_5m`, `market_cap`, `price`) into `metadata["pumpfun"]` for downstream services.【F:solhunter_zero/token_scanner.py†L1708-L1754】

## Example leaderboard payload
```json
[
  {
    "mint": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
    "tokenMint": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
    "name": "Cat Cash",
    "symbol": "CASH",
    "image_url": "https://cdn.pumpportal.fun/icons/cash.png",
    "imageUrl": "https://cdn.pumpportal.fun/icons/cash.png",
    "liquidity": 48231.22,
    "volume_24h": 913245.77,
    "volume_1h": 58231.04,
    "volume_5m": 9421.12,
    "market_cap": 1823045.91,
    "price": 0.0142
  }
]
```

## Retry/backoff rules
- Momentum agent enforces a token-bucket limiter at 3 rps with burst 3 for `pump.fun`, plus host-level circuit breakers that back off for 60 seconds after repeated failures. Cached responses persist for `_SOCIAL_CACHE_TTL_SEC` (5 minutes).【F:solhunter_zero/golden_pipeline/momentum.py†L60-L116】【F:solhunter_zero/golden_pipeline/momentum.py†L749-L821】【F:solhunter_zero/golden_pipeline/momentum.py†L1718-L1740】
- Discovery fallbacks log provider errors and short-circuit when Pump.fun data is unavailable to avoid blocking DAS flows.【F:solhunter_zero/token_scanner.py†L1237-L1374】

## Testing endpoint command
```bash
curl -s "${PUMP_LEADERBOARD_URL:-https://pumpportal.fun/api/leaderboard}" \
  --get \
  --data-urlencode "sort=${PUMP_LEADERBOARD_SORT:-volume_24h}" \
  --data-urlencode "timeframe=${PUMP_LEADERBOARD_TIMEFRAME:-24h}" \
  --data-urlencode "limit=20"
```
