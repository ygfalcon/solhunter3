# Pyth Integration

## API base URL
- Uses Hermes REST endpoints; default `PYTH_PRICE_URL` is `https://hermes.pyth.network/v2/updates/price/latest`. Override with `PYTH_HERMES_PRICE_URL` for depth adapters or `PYTH_PRICE_URL` for direct price pulls.【F:solhunter_zero/prices.py†L174-L204】【F:solhunter_zero/golden_pipeline/depth_adapter.py†L49-L62】

## Expected JSON payload
- Requests are HTTP GETs with repeated `ids[]=<feed_id>` parameters for every configured price feed resolved from `PYTH_PRICE_IDS`. Responses include either a top-level `parsed` array or a bare list of entries, each with `id`, `price.publish_time`, `price.price`, and `price.conf` fields.【F:solhunter_zero/prices.py†L1180-L1233】
- Golden depth adapters reuse Hermes `/api/latest_price_feeds` to hydrate synthetic depth snapshots.【F:solhunter_zero/synthetic_depth.py†L41-L86】【F:solhunter_zero/golden_pipeline/depth_adapter.py†L451-L525】

## Retry/backoff rules
- Price fetches use the shared `_request_json` helper with `PRICE_RETRY_ATTEMPTS` (default 3) and exponential backoff (`PRICE_RETRY_BACKOFF`, default 0.5s) plus jitter. Connector-level timeouts derive from `PYTH_CONNECT_TIMEOUT`, `PYTH_READ_TIMEOUT`, and `PYTH_TOTAL_TIMEOUT`.【F:solhunter_zero/prices.py†L48-L75】【F:solhunter_zero/prices.py†L684-L735】【F:solhunter_zero/prices.py†L1180-L1206】
- Host-level concurrency is guarded by the global controller (`hermes.pyth.network` limit 6, cooldown 20s).【F:solhunter_zero/http.py†L264-L306】

## Schema sample
```http
GET /v2/updates/price/latest?ids[]=J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS
Accept: application/json
```
```json
{
  "parsed": [
    {
      "id": "J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS",
      "price": {
        "price": "23.4512",
        "conf": "0.0123",
        "expo": -2,
        "publish_time": 1716509123
      }
    }
  ]
}
```

## Testing endpoint command
```bash
curl -s "${PYTH_PRICE_URL:-https://hermes.pyth.network/v2/updates/price/latest}?ids[]=J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS"
```
