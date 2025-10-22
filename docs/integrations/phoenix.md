# Phoenix Integration

## API base URL
- REST base defaults to `https://api.phoenix.trade` (`PHOENIX_API_URL`) for price snapshots at `/price?token=<mint>`. Websocket endpoint is configured via `PHOENIX_WS_URL` for continuous depth streaming when available.【F:solhunter_zero/arbitrage.py†L288-L336】【F:solhunter_zero/services.py†L65-L81】

## Expected JSON payload
- Price endpoint replies with `{ "price": <float> }`, cached in the arbitrage layer alongside other venues.【F:solhunter_zero/arbitrage.py†L952-L988】
- Phoenix venue detection tags discovery metadata and surfaces program-specific pools in UI tooling.【F:solhunter_zero/scanner_onchain.py†L1112-L1136】

## Retry/backoff rules
- Requests honour shared host retry/backoff behaviour; breakers open after repeated failures and cooldown before new attempts. Streaming clients reconnect with logged warnings if the websocket drops.【F:solhunter_zero/token_discovery.py†L520-L612】【F:solhunter_zero/arbitrage.py†L1177-L1325】

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
curl -s "${PHOENIX_API_URL:-https://api.phoenix.trade}/price?token=So11111111111111111111111111111111111111112"
```
