# Helius DAS Integration

## API base URL
- Default base URL: `https://mainnet.helius-rpc.com`. Override with `DAS_BASE_URL` and embed an `api-key` query parameter when keys are provided via `HELIUS_API_KEY(S)` or inline on the URL.【F:solhunter_zero/clients/helius_das.py†L72-L124】

## Expected JSON payload
- `searchAssets` requests are JSON-RPC POST bodies containing `page`, `limit`, `sortBy`, optional `interface="FungibleToken"`, and cursor hints. The helper automatically strips empty values and normalises mint lists before dispatch.【F:solhunter_zero/clients/helius_das.py†L226-L287】【F:solhunter_zero/clients/helius_das.py†L337-L417】
- `getAssetBatch` issues the `getAssetBatch` method with an `ids` array filtered to valid base58 mints.【F:solhunter_zero/clients/helius_das.py†L360-L408】

## Retry/backoff rules
- Token bucket rate limiter gates calls at `DAS_RPS` (default 2 rps) with a configurable burst size.【F:solhunter_zero/clients/helius_das.py†L126-L149】
- Each request retries up to `_MAX_RETRIES` (default 2) with exponential backoff capped by `_BACKOFF_CAP` and honours 429 `Retry-After` headers.【F:solhunter_zero/clients/helius_das.py†L207-L323】

## Schema sample
```json
{
  "jsonrpc": "2.0",
  "method": "searchAssets",
  "params": {
    "page": 1,
    "limit": 60,
    "interface": "FungibleToken",
    "sortBy": "lastActivatedTime"
  }
}
```
Responses include a top-level `result` mapping with `items` (asset metadata) and `nextCursor`. Each `item` carries `id`, `content.metadata` (name, symbol, mint), and DAS `token_info` fields consumed by discovery.【F:solhunter_zero/scanner_onchain.py†L1188-L1209】

## Testing endpoint command
```bash
curl -s -X POST "${DAS_BASE_URL:-https://mainnet.helius-rpc.com}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"search","method":"searchAssets","params":{"page":1,"limit":5,"interface":"FungibleToken","sortBy":"lastActivatedTime"}}'
```
