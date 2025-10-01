import json
import os
import subprocess
import sys

import pytest


@pytest.mark.timeout(60)
def test_demo_full_preset_output(tmp_path):
    report_dir = tmp_path / "reports"
    env = dict(os.environ, SOLHUNTER_PATCH_INVESTOR_DEMO="1")
    subprocess.run(
        [sys.executable, "demo.py", "--preset", "full", "--reports", str(report_dir)],
        check=True,
        env=env,
    )

    agg = json.loads((report_dir / "aggregate_summary.json").read_text())
    assert agg["global_roi"] == pytest.approx(2.3452307692307692)

    token_roi = {item["token"]: item["roi"] for item in agg["per_token"]}
    assert token_roi["ARB"] == pytest.approx(1.3076923076923075)
    assert token_roi["MOM"] == pytest.approx(5.0)
    assert token_roi["REV"] == pytest.approx(0.728)
