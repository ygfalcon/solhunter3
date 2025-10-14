from __future__ import annotations

import json
import time
import urllib.request
import urllib.error

from .util.env import optional_helius_rpc_url


def ensure_rpc(*, warn_only: bool = False) -> None:
    """Send a simple JSON-RPC request to ensure the Solana RPC is reachable."""
    rpc_url = optional_helius_rpc_url("")
    if not rpc_url:
        if warn_only:
            print("Warning: SOLANA_RPC_URL/HELIUS_RPC_URL not configured; skipping RPC check")
            return
        raise RuntimeError("SOLANA_RPC_URL or HELIUS_RPC_URL must be configured")

    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getHealth"}).encode()
    req = urllib.request.Request(
        rpc_url, data=payload, headers={"Content-Type": "application/json"}
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:  # nosec B310
                resp.read()
                return
        except urllib.error.HTTPError as exc:  # pragma: no cover - provider quirks
            # Helius and some proxies respond with 404 to POST /?api-key=...
            # We treat any <500 response as reachability success to avoid
            # aborting startup when the RPC is otherwise healthy.
            if 200 <= getattr(exc, "code", 500) < 500:
                try:
                    exc.read()
                except Exception:
                    pass
                return
            last_exc = exc
        except Exception as exc:  # pragma: no cover - network failure
            last_exc = exc

        if attempt == 2:
            msg = (
                f"Failed to contact Solana RPC at {rpc_url} after 3 attempts: {last_exc}."
                " Please ensure the endpoint is reachable or set SOLANA_RPC_URL to a valid RPC."
            )
            print(f"Warning: {msg}")
            return
        wait = 2 ** attempt
        print(
            f"Attempt {attempt + 1} failed to contact Solana RPC at {rpc_url}: {last_exc}.",
            f" Retrying in {wait} seconds...",
        )
        time.sleep(wait)
