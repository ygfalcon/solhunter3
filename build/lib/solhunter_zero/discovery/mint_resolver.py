from __future__ import annotations

from typing import Optional
import re

ROUTER_OR_PROGRAM_HINTS = {
    # common non-mint accounts people accidentally feed in
    "JUP4F": "router",   # Jupiter v4 router prefix (example)
    "Pump111": "program",
}

# quick heuristic: SPL mints are base58, 32 bytes; but we still must verify via DAS/TokenList
_B58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

def looks_like_pubkey(s: str) -> bool:
    return bool(_B58_RE.match(s))

def likely_program_or_router(s: str) -> bool:
    # flag known router/program prefixes; *not* authoritative
    return any(s.startswith(prefix) for prefix in ROUTER_OR_PROGRAM_HINTS)

def normalize_candidate(addr: str) -> Optional[str]:
    """
    Returns a *candidate* key we'll try as a mint. Filters obvious program/router IDs.
    """
    if not looks_like_pubkey(addr):
        return None
    if likely_program_or_router(addr):
        return None
    return addr
