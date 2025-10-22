# Jupiter Integration

## API base URL
- REST base defaults to `https://lite-api.jup.ag` with endpoints `/swap/v1/quote`, `/swap/v1/swap`, and `/tokens/v2/search`. Override with `JUPITER_API_BASE`, `JUPITER_QUOTE_URL`, `JUPITER_SWAP_URL`, or `JUPITER_TOKEN_SEARCH_URL` as needed.【F:solhunter_zero/swap/jupiter.py†L8-L46】
- Websocket price stream defaults to `wss://stats.jup.ag/ws`, configurable through `JUPITER_WS_URL`.【F:solhunter_zero/services.py†L23-L76】【F:solhunter_zero/scanner_common.py†L56-L79】

## Expected JSON payload
- Quote requests are HTTP GETs with `inputMint`, `outputMint`, `amount`, `slippageBps`, and `swapMode=ExactIn`. Responses include `outAmount`, `priceImpactPct`, `routePlan`, and metadata consumed by execution.【F:solhunter_zero/swap/jupiter.py†L73-L127】
- Swap transactions are POSTed to `/swap/v1/swap` with the selected route; the response returns a base64-encoded `swapTransaction` blob ready for signing.【F:solhunter_zero/swap/jupiter.py†L129-L204】
- Token search accepts `query=<symbol|mint>` and returns an array of matches with `address` and metadata, which the loader normalises to canonical mints.【F:solhunter_zero/swap/jupiter.py†L99-L124】

## Retry/backoff rules
- REST calls honour per-endpoint `ClientTimeout` envelopes derived from `JUPITER_SWAP_CONNECT_TIMEOUT`, `JUPITER_SWAP_READ_TIMEOUT`, and `JUPITER_SWAP_TOTAL_TIMEOUT`. Retries are delegated to higher-level price polling, which uses the shared `_request_json` helper with exponential backoff (`PRICE_RETRY_ATTEMPTS`, `PRICE_RETRY_BACKOFF`).【F:solhunter_zero/swap/jupiter.py†L30-L46】【F:solhunter_zero/prices.py†L48-L75】【F:solhunter_zero/prices.py†L684-L735】
- Websocket consumers reconnect on failure with venue-specific logging and reuse the configured URL.【F:solhunter_zero/arbitrage.py†L1267-L1325】

## Schema sample
```http
GET /swap/v1/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=Es9vMFrzaCERNNjXzjCFKDc9GjL2dgNp7G1h9j9wUoB9&amount=1000000&slippageBps=50&swapMode=ExactIn
Accept: application/json
```
```json
{
  "inputMint": "So11111111111111111111111111111111111111112",
  "outputMint": "Es9vMFrzaCERNNjXzjCFKDc9GjL2dgNp7G1h9j9wUoB9",
  "inAmount": "1000000",
  "outAmount": "2498000",
  "priceImpactPct": "0.0042",
  "routePlan": [
    { "swapInfo": { "ammKey": "...", "label": "Orca" } }
  ]
}
```

## Testing endpoint command
```bash
curl -s "${JUPITER_QUOTE_URL:-https://lite-api.jup.ag/swap/v1/quote}?inputMint=So11111111111111111111111111111111111111112&outputMint=Es9vMFrzaCERNNjXzjCFKDc9GjL2dgNp7G1h9j9wUoB9&amount=1000000&swapMode=ExactIn&slippageBps=50"
```
