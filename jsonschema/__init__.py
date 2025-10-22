"""Minimal jsonschema stub for offline testing."""

from __future__ import annotations

from .exceptions import ValidationError


class Draft202012Validator:
    """Stub validator that accepts all inputs."""

    def __init__(self, schema: object) -> None:
        self.schema = schema

    def validate(self, instance: object) -> None:
        return None


__all__ = ["Draft202012Validator", "ValidationError"]
