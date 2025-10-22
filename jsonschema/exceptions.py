"""Exceptions for the jsonschema stub."""

from __future__ import annotations


class ValidationError(Exception):
    def __init__(self, message: str, *, instance: object | None = None, schema: object | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.instance = instance
        self.schema = schema


__all__ = ["ValidationError"]
