from __future__ import annotations

"""Compatibility wrappers around :mod:`token_scanner`."""

from .token_scanner import TokenScanner, scan_tokens, scan_tokens_async, OFFLINE_TOKENS

__all__ = ["TokenScanner", "scan_tokens", "scan_tokens_async", "OFFLINE_TOKENS"]
