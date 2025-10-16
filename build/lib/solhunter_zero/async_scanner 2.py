from __future__ import annotations

"""Compatibility shim for :mod:`token_scanner`."""

from .token_scanner import TokenScanner, scan_tokens_async

__all__ = ["TokenScanner", "scan_tokens_async"]
