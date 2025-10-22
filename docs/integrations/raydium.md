# Raydium Integration

## API base URL
- REST base defaults to `https://api.raydium.io` (`RAYDIUM_API_URL`) for swap prices at `/price?token=<mint>`. Websocket URL is supplied via runtime settings (`RAYDIUM_WS_URL`) for live book depth when enabled.【F:solhunter_zero/arbitrage.py†L287-L336】【F:solhunter_zero/services.py†L65-L81】

## Expected JSON payload
- Price endpoint returns `{ "price": <float> }`, cached by arbitrage and merged into shared price caches for execution fallbacks.【F:solhunter_zero/arbitrage.py†L936-L972】
- Venue tagging marks Raydium pools based on account metadata so UI panels show per-venue depth.【F:solhunter_zero/scanner_onchain.py†L1114-L1136】

## Retry/backoff rules
- Requests reuse the shared discovery HTTP helper with host-level retry limits; when repeated errors occur, breaker state prevents further Raydium pulls until cooldown expires. Websocket clients reconnect automatically with log notices.【F:solhunter_zero/token_discovery.py†L520-L612】【F:solhunter_zero/arbitrage.py†L1177-L1325】

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
curl -s "${RAYDIUM_API_URL:-https://api.raydium.io}/price?token=So11111111111111111111111111111111111111112"
```
