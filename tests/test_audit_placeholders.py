from pathlib import Path

from scripts.audit_placeholders import scan_for_placeholders


def test_scan_for_placeholders_detects_patterns(tmp_path: Path):
    target = tmp_path / "config.txt"
    target.write_text("API_KEY=YOUR_SECRET\n")
    matches = scan_for_placeholders([target])
    assert matches
    assert matches[0].path.name == "config.txt"


def test_scan_for_placeholders_allows_clean_files(tmp_path: Path):
    target = tmp_path / "config.txt"
    target.write_text("API_KEY=real_secret\n")
    matches = scan_for_placeholders([target])
    assert matches == []
