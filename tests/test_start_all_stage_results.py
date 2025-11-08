import pytest

from scripts.start_all import StageResult, run_stage


def test_run_stage_records_system_exit():
    stage_results: list[StageResult] = []

    def boom():
        raise SystemExit("threshold not met")

    with pytest.raises(SystemExit):
        run_stage("boom", boom, stage_results)

    assert stage_results
    entry = stage_results[0]
    assert entry.name == "boom"
    assert not entry.success
    assert entry.error == "threshold not met"
