# Orca Integration

## API base URL
- REST base defaults to `https://api.orca.so` (`ORCA_API_URL`) for price snapshots at `/price?token=<mint>`. Websocket streaming uses the configured `ORCA_WS_URL` published through runtime manifests for live liquidity updates.【F:solhunter_zero/arbitrage.py†L286-L336】【F:solhunter_zero/services.py†L65-L81】

## Expected JSON payload
- Price endpoint returns `{ "price": <float> }`, which is cached inside the arbitrage module and propagated to consolidated price caches.【F:solhunter_zero/arbitrage.py†L911-L936】
- Discovery heuristics tag venues by inspecting mint metadata; Orca program IDs set `venues.add("orca")` for UI display.【F:solhunter_zero/scanner_onchain.py†L1112-L1136】

## Retry/backoff rules
- Host guard rails inherited from the shared HTTP controller (`api.orca.so` falls under default retry policy of two attempts with exponential backoff via `host_retry_config`). Live websocket clients reconnect on disconnect with info-level logs for traceability.【F:solhunter_zero/token_discovery.py†L520-L612】【F:solhunter_zero/arbitrage.py†L1177-L1325】

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
curl -s "${ORCA_API_URL:-https://api.orca.so}/price?token=So11111111111111111111111111111111111111112"
```
