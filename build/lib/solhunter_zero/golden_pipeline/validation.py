"""Schema validation helpers for Golden pipeline message streams."""

from __future__ import annotations

import logging
from collections.abc import Mapping as AbcMapping
from typing import Any, Dict, Mapping

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from solhunter_zero.schemas.golden_pipeline import STREAM_SCHEMAS

log = logging.getLogger(__name__)

_VALIDATORS: Dict[str, Draft202012Validator] = {
    stream: Draft202012Validator(schema) for stream, schema in STREAM_SCHEMAS.items()
}


def _normalise(value: Any) -> Any:
    if isinstance(value, AbcMapping):
        return {key: _normalise(val) for key, val in value.items()}
    if isinstance(value, tuple):
        return [_normalise(item) for item in value]
    if isinstance(value, list):
        return [_normalise(item) for item in value]
    return value


class SchemaValidationError(ValueError):
    """Raised when a payload fails JSON Schema validation."""

    def __init__(self, stream: str, error: ValidationError) -> None:
        message = f"{stream} payload failed validation: {error.message}"
        super().__init__(message)
        self.stream = stream
        self.error = error


def validate_stream_payload(stream: str, payload: Mapping[str, object]) -> Dict[str, object]:
    """Return a materialised payload after validating against ``stream`` schema."""

    schema = STREAM_SCHEMAS.get(stream)
    if schema is None:
        return dict(payload)
    if not isinstance(payload, Mapping):
        error = ValidationError("payload must be a mapping", instance=payload, schema=schema)
        raise SchemaValidationError(stream, error)
    validator = _VALIDATORS[stream]
    materialised = _normalise(dict(payload))
    try:
        validator.validate(materialised)
    except ValidationError as exc:
        log.debug("validation failed for stream %s: %s", stream, exc, exc_info=True)
        raise SchemaValidationError(stream, exc) from exc
    return materialised


__all__ = ["SchemaValidationError", "validate_stream_payload"]
