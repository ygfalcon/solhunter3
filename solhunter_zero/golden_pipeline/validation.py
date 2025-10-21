"""Runtime validation for Golden pipeline bus payloads."""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from importlib import resources
from typing import Any, Mapping, MutableMapping

from jsonschema import Draft202012Validator, ValidationError

from .contracts import STREAMS


log = logging.getLogger(__name__)

_SCHEMA_DIRECTORY = "bus_schemas"

_STREAM_TO_SCHEMA: MutableMapping[str, str] = {
    STREAMS.golden_snapshot: "golden_snapshot.schema.json",
    STREAMS.trade_suggested: "trade_suggestion.schema.json",
    STREAMS.vote_decisions: "vote_decision.schema.json",
    STREAMS.virtual_fills: "virtual_fill.schema.json",
    STREAMS.live_fills: "live_fill.schema.json",
    "agent.suggestion": "trade_suggestion.schema.json",
    "agent.vote": "vote_decision.schema.json",
    "execution.shadow.fill": "virtual_fill.schema.json",
}


@lru_cache(maxsize=None)
def _load_validator(filename: str) -> Draft202012Validator:
    base = resources.files("solhunter_zero.static")
    schema_path = base.joinpath(_SCHEMA_DIRECTORY).joinpath(filename)
    schema_text = schema_path.read_text("utf-8")
    schema = json.loads(schema_text)
    return Draft202012Validator(schema)


def validate_stream_payload(stream: str, payload: Mapping[str, Any] | Any) -> bool:
    """Validate ``payload`` for ``stream`` against the registered schema."""

    schema_name = _STREAM_TO_SCHEMA.get(stream)
    if not schema_name:
        return True
    if not isinstance(payload, Mapping):
        log.error(
            "Rejected payload for stream %s: expected mapping but received %s",
            stream,
            type(payload).__name__,
        )
        return False
    validator = _load_validator(schema_name)
    materialised = dict(payload)
    try:
        validator.validate(materialised)
    except ValidationError as exc:  # pragma: no cover - defensive logging
        sample = {key: materialised[key] for key in list(materialised)[:6]}
        log.error(
            "Rejected payload for stream %s: %s", stream, exc.message, extra={"payload_sample": sample}
        )
        return False
    return True


__all__ = ["validate_stream_payload"]

