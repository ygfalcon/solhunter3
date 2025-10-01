from scripts import mac_setup


def test_ensure_profile_idempotent(tmp_path, monkeypatch):
    # Set up fake HOME and SHELL
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("SHELL", "/bin/bash")
    profile = tmp_path / ".bash_profile"

    # Fake brew shellenv output
    brew_env = 'export HOMEBREW_PREFIX="/custom"\nexport PATH="/custom/bin:$PATH"'
    monkeypatch.setattr(mac_setup.subprocess, "check_output", lambda cmd, text=True: brew_env)

    # First run
    mac_setup.ensure_profile()
    content_first = profile.read_text()

    # Second run should not duplicate entries
    mac_setup.ensure_profile()
    content_second = profile.read_text()

    assert content_first == content_second
    assert content_second.count('HOMEBREW_PREFIX') == 1
    assert content_second.count('source "$HOME/.cargo/env"') == 1
