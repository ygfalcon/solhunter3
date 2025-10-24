# Status Memo: Live Console Refactor

## Summary
- Refactor halted due to token limit exhaustion during Live Console implementation.
- Live Console redesign requirements documented in latest directive; no code changes committed in this session.

## Remaining Tasks
1. Resume refactor ensuring single Live Console with ten specified panels wired to live endpoints.
2. Implement shared `selectedMint` store driving Discovery Stream selections.
3. Ensure pause/resume, windowing, optimistic UI, and error handling across all panels.
4. Build Self-Check harness verifying panel wiring, mint propagation, PnL computation, and provider health metrics.
5. Confirm console logs "UI ready: 10/10 panels wired, no dead panels" after Self-Check passes.

## Notes
- Production environment endpoints: event bus `ws://127.0.0.1:8779` (channel `solhunter-events-v3`), Redis `redis://localhost:6379/1`.
- Environment config: `etc/solhunter/env.production` with `SOLHUNTER_MODE=live`; respect `KEYPAIR_PATH` and surface fingerprint in header.
- Remove legacy screens; enforce type-safe parsing and inline deserialization error reporting.

