# Evaluation: Replacing raw RPC scans with Helius DAS `searchAssets`

## Context

The current `scan_tokens_onchain` flow in `solhunter_zero/scanner_onchain.py` performs capped `getProgramAccounts` scans against the configured Solana RPC endpoint. It fetches mint and token accounts in two passes, applies manual pagination, and optionally hydrates basic liquidity/volume metrics once mint candidates are collected.【F:solhunter_zero/scanner_onchain.py†L495-L607】 The function sits behind backoff guards to avoid repeatedly issuing heavy scans when providers reject requests, but it still relies on direct RPC calls implemented via bespoke helpers such as `_perform_program_scan` and `_fetch_program_accounts_v2_helius`.

A DAS-based client has been proposed (see `solhunter_zero/clients/helius_das.py`) that would shift discovery to `searchAssets`/`getAssetBatch`, throttled by a coroutine-aware rate limiter. The implementation uses module-level configuration (API key, rate limits) and expects to run inside an `aiohttp.ClientSession`.

## Fit with existing architecture

### HTTP client lifecycle

Solhunter centralises HTTP configuration in `solhunter_zero/http.py`, providing a shared `aiohttp` session with connector options (custom resolver, IPv4 forcing, user agent) for the active event loop.【F:solhunter_zero/http.py†L158-L205】 The suggested DAS helper creates ad-hoc sessions, bypassing that infrastructure. Integrating it cleanly would either require refactoring the helper to accept an externally managed session (preferably obtained from `http.get_session()`) or duplicating connection settings and DNS overrides. The latter would diverge from the existing policy surface.

### Discovery pipeline expectations

Downstream code expects `scan_tokens_onchain` to return a list of mint addresses (or dictionaries containing `address`, `liquidity`, and `volume` when metrics are requested).【F:solhunter_zero/scanner_onchain.py†L555-L607】 The DAS helper emits richer asset objects. We would need a translation layer to map `searchAssets` responses back to the minimal shape the pipeline consumes, or we would need to revisit consumers such as the discovery agent, which combines on-chain mints with BirdEye and other sources.【F:solhunter_zero/token_discovery.py†L67-L160】 Without that alignment we risk shape mismatches in existing flows and tests.

### Backoff and rate limiting

`scan_tokens_onchain` already employs backoff when Helius denies heavy scans, tracking failures in a TTL cache so discovery skips repeated RPC attempts until the window expires.【F:solhunter_zero/scanner_onchain.py†L511-L523】 Moving to DAS would eliminate the need for program account pagination and should reduce RPC load, but we must ensure the new rate limiter plays well with the discovery cadence. The proposed `_rl` limits requests to approximately two per second. In the fast pipeline mode the discovery loop can demand dozens of mints per cycle; we need to confirm that the throttled DAS calls can keep up or adjust concurrency (perhaps by widening `burst` based on `max_tokens`).

### Data completeness and validation

`searchAssets` prioritises "recentAction" ordering, which aligns with the goal of discovering newly active fungible tokens. However, the current RPC scan deliberately over-fetches mints (`_ONCHAIN_SCAN_OVERFETCH`) to compensate for filtering stages and ensures uniqueness across mint/token account passes.【F:solhunter_zero/scanner_onchain.py†L452-L550】 When switching to DAS we should validate that cursor-based pagination yields enough fresh entries to feed the same heuristics, especially when BirdEye throttles and on-chain discovery becomes the fallback. Additionally, `getAssetBatch` can verify decimals/symbols, but we should benchmark whether the combined latency still fits inside the discovery SLA.

### Environment and configuration surface

The repository already exposes `HELIUS_API_KEY` across price and token modules.【F:solhunter_zero/token_scanner.py†L30-L40】【F:solhunter_zero/prices.py†L20-L223】 Introducing `HELIUS_API_TOKEN` or `DAS_DISCOVERY_LIMIT` needs coordination with configuration docs and startup checks to avoid diverging environment requirements. We may also want to reuse existing knobs such as `ONCHAIN_SCAN_MIN_ACCOUNTS` instead of introducing parallel DAS-specific limits, or at least document how they interact.

## Recommendation

Replacing the raw RPC scan with DAS search feels promising—it eliminates the heaviest RPC calls, provides richer metadata, and should respect network policies better. To adopt it safely we should:

1. Refactor the helper into a `clients` package that relies on `http.get_session()` for connection reuse and config inheritance.
2. Introduce an adapter layer so `scan_tokens_onchain` (or its successor) keeps the same return contract while sourcing mint candidates from `searchAssets`.
3. Extend existing backoff/metrics logic to the DAS path, including environment documentation updates.
4. Benchmark discovery throughput under the proposed rate limiter to ensure fast-pipeline runs remain responsive.

Until those pieces are in place, the current architecture continues to operate albeit with the noted rate-limit risk. Implementing DAS as an optional discovery path first (feature-flagged) could provide operational validation before we retire the RPC scan entirely.

## Implementation status

The new Helius client now lives under `solhunter_zero/clients/helius_das.py`, exposing search/batch helpers that reuse the shared HTTP session and respect a coroutine-aware rate limiter.【F:solhunter_zero/clients/helius_das.py†L1-L121】 The on-chain scanner prefers DAS when `ONCHAIN_USE_DAS=1` (or `ONCHAIN_DISCOVERY_PROVIDER=das`), translating `searchAssets` responses back into mint lists and, when metrics are requested, enriching them with optional DAS metadata before invoking the existing liquidity/volume collectors.【F:solhunter_zero/scanner_onchain.py†L365-L526】【F:solhunter_zero/scanner_onchain.py†L540-L618】 If DAS yields no candidates or raises, the legacy RPC pagination path remains as a safety net.【F:solhunter_zero/scanner_onchain.py†L618-L684】
