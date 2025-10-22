import pytest

from solhunter_zero.golden_pipeline.validation import validate_stream_payload

from .conftest import STREAMS


@pytest.mark.smoke
def test_schema_validation_smoke(golden_harness, fake_broker) -> None:
    events = fake_broker.events[STREAMS.golden_snapshot]
    assert events, "expected at least one golden snapshot"

    payload = validate_stream_payload(STREAMS.golden_snapshot, events[-1])
    assert payload["schema_version"], "missing schema version"
    assert payload["mint"], "missing mint"
    assert payload["px"].get("mid_usd")
