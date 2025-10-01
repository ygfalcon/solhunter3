import subprocess
import sys


def test_make_start_help():
    result = subprocess.run(
        ['make', 'start', 'ARGS=--help', f'PYTHON={sys.executable}'],
        capture_output=True, text=True
    )
    assert result.returncode == 0
    out = result.stdout.lower() + result.stderr.lower()
    assert 'usage' in out
