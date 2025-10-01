import os
import subprocess
import sys
from pathlib import Path


def test_mac_setup_aborts_without_tools(tmp_path):
    script_src = Path('scripts/mac_setup.py').read_text()
    scripts_dir = tmp_path / 'scripts'
    scripts_dir.mkdir()
    mac_setup = scripts_dir / 'mac_setup.py'
    mac_setup.write_text(script_src)
    mac_setup.chmod(0o755)

    homebrew = tmp_path / 'homebrew'
    bin_dir = homebrew / 'bin'
    bin_dir.mkdir(parents=True)

    brew = bin_dir / 'brew'
    brew.write_text(f"""#!/usr/bin/env bash
if [ \"$1\" = \"--prefix\" ]; then
  echo \"{homebrew}\"
elif [ \"$1\" = \"shellenv\" ]; then
  echo 'export PATH="{bin_dir}:$PATH"'
else
  exit 0
fi
""")
    brew.chmod(0o755)

    xcode = bin_dir / 'xcode-select'
    xcode.write_text("#!/usr/bin/env bash\nexit 0\n")
    xcode.chmod(0o755)

    rustup_init = bin_dir / 'rustup-init'
    rustup_init.write_text("#!/usr/bin/env bash\nexit 0\n")
    rustup_init.chmod(0o755)

    cargo_dir = tmp_path / '.cargo'
    cargo_dir.mkdir()
    (cargo_dir / 'env').write_text('')

    env = os.environ.copy()
    env['PATH'] = f"{bin_dir}:/usr/bin:/bin"
    env['HOME'] = str(tmp_path)
    env['SHELL'] = '/bin/bash'
    env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])

    result = subprocess.run([sys.executable, str(mac_setup), '--non-interactive'], cwd=tmp_path, env=env, capture_output=True, text=True)

    assert result.returncode != 0
    assert 'Missing rustup' in result.stdout
    profile = tmp_path / '.bash_profile'
    assert not profile.exists()
