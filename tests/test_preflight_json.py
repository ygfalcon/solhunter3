import json
import pytest
from scripts import preflight


def test_preflight_writes_json(tmp_path, monkeypatch):
    monkeypatch.setattr(preflight, "ROOT", tmp_path)

    def ok_check():
        return True, "ok"

    def fail_check():
        return False, "nope"

    monkeypatch.setattr(preflight, "CHECKS", [("ok", ok_check), ("fail", fail_check)])

    json_path = tmp_path / "preflight.json"
    json_path.write_text("old")

    with pytest.raises(SystemExit) as exc:
        preflight.main()

    assert exc.value.code == 1
    data = json.loads(json_path.read_text())
    assert data["successes"] == [{"name": "ok", "message": "ok"}]
    assert data["failures"] == [{"name": "fail", "message": "nope"}]
