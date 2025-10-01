import os
import platform
import pytest

os.environ.setdefault("TORCH_METAL_VERSION", "2.8.0")
os.environ.setdefault("TORCHVISION_METAL_VERSION", "0.19.0")

from solhunter_zero import bootstrap_utils


def test_ensure_deps_runs_prepare_macos_env(monkeypatch):
    marker = bootstrap_utils.DEPS_MARKER
    marker.unlink(missing_ok=True)
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    called = {"prepare": 0}

    def fake_prepare_macos_env(non_interactive=True):
        called["prepare"] += 1
        return {"success": True}

    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", fake_prepare_macos_env
    )
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.mac_setup_completed", lambda: False
    )
    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: ([], []))
    monkeypatch.setattr("solhunter_zero.bootstrap.ensure_target", lambda name: None)

    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)

    assert called["prepare"] == 1


def test_mac_setup_marker_skips_prepare(monkeypatch):
    marker = bootstrap_utils.DEPS_MARKER
    marker.unlink(missing_ok=True)
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    called = {"prepare": 0}

    def fake_prepare_macos_env(non_interactive=True):
        called["prepare"] += 1
        return {"success": True}

    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", fake_prepare_macos_env
    )
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.mac_setup_completed", lambda: True
    )
    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: ([], []))
    monkeypatch.setattr("solhunter_zero.bootstrap.ensure_target", lambda name: None)

    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)

    assert called["prepare"] == 0


def test_deps_marker_skips_install(monkeypatch):
    marker = bootstrap_utils.DEPS_MARKER
    marker.unlink(missing_ok=True)

    calls: list[tuple] = []

    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: (["req"], []))
    monkeypatch.setattr(bootstrap_utils, "_package_missing", lambda pkg: True)
    monkeypatch.setattr(
        bootstrap_utils, "_pip_install", lambda *a, **k: calls.append(a)
    )
    monkeypatch.setattr("solhunter_zero.bootstrap.ensure_target", lambda name: None)
    import types, sys
    dummy_preflight = types.SimpleNamespace(check_internet=lambda: (True, "ok"))
    monkeypatch.setitem(sys.modules, "scripts.preflight", dummy_preflight)
    import importlib
    orig_find_spec = importlib.util.find_spec

    def fake_find_spec(name):
        if name == "req":
            return object()
        return orig_find_spec(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)

    assert marker.exists()
    assert calls

    calls.clear()

    def fail_check():
        raise AssertionError("check_deps should be skipped")

    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", fail_check)
    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)

    assert not calls
    marker.unlink(missing_ok=True)


def test_force_env_var_reinstalls(monkeypatch):
    marker = bootstrap_utils.DEPS_MARKER
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("ok")
    calls: list[tuple] = []

    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: (["req"], []))
    monkeypatch.setattr(bootstrap_utils, "_package_missing", lambda pkg: True)
    monkeypatch.setattr(
        bootstrap_utils, "_pip_install", lambda *a, **k: calls.append(a)
    )
    monkeypatch.setattr("solhunter_zero.bootstrap.ensure_target", lambda name: None)
    monkeypatch.setenv("SOLHUNTER_FORCE_DEPS", "1")
    import types, sys
    dummy_preflight = types.SimpleNamespace(check_internet=lambda: (True, "ok"))
    monkeypatch.setitem(sys.modules, "scripts.preflight", dummy_preflight)
    import importlib
    orig_find_spec = importlib.util.find_spec

    def fake_find_spec(name):
        if name == "req":
            return object()
        return orig_find_spec(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)

    assert calls
    marker.unlink(missing_ok=True)


def test_redis_broker_requires_server(monkeypatch, capsys):
    marker = bootstrap_utils.DEPS_MARKER
    marker.unlink(missing_ok=True)
    monkeypatch.setenv("BROKER_URL", "redis://localhost")
    monkeypatch.setattr(bootstrap_utils.shutil, "which", lambda cmd: None)
    with pytest.raises(SystemExit):
        bootstrap_utils.ensure_deps(ensure_wallet_cli=False)
    out = capsys.readouterr().out
    assert "redis-server" in out
    assert "BROKER_URL=memory://" in out


@pytest.mark.parametrize(
    "env_var,url,expected",
    [
        ("BROKER_URL", "redis://localhost", "redis"),
        ("BROKER_URLS", "nats://localhost", "nats-py"),
    ],
)
def test_broker_packages_installed(monkeypatch, env_var, url, expected):
    marker = bootstrap_utils.DEPS_MARKER
    marker.unlink(missing_ok=True)
    monkeypatch.delenv("BROKER_URL", raising=False)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    monkeypatch.setenv(env_var, url)
    installed: list[str] = []
    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: ([], []))
    monkeypatch.setattr(bootstrap_utils, "_package_missing", lambda pkg: True)
    monkeypatch.setattr(
        "solhunter_zero.bootstrap.ensure_target", lambda name: None
    )
    import types, sys

    dummy_preflight = types.SimpleNamespace(check_internet=lambda: (True, "ok"))
    monkeypatch.setitem(sys.modules, "scripts.preflight", dummy_preflight)
    monkeypatch.setattr(
        bootstrap_utils,
        "_pip_install",
        lambda *a, **k: installed.append(a[0]),
    )
    if url.startswith("redis://"):
        monkeypatch.setattr(
            bootstrap_utils.shutil, "which", lambda cmd: "/usr/bin/redis-server"
        )
    bootstrap_utils.ensure_deps(ensure_wallet_cli=False)
    assert expected in installed
