"""Minimal base58 stub for offline demos."""

from __future__ import annotations


def b58decode(value: str | bytes, *args, **kwargs) -> bytes:
    if isinstance(value, bytes):
        return value
    return value.encode("utf-8")


def b58encode(value: bytes | str, *args, **kwargs) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


__all__ = ["b58decode", "b58encode"]
