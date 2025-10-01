import importlib
import sys
import tomllib
import types

import pytest


def test_load_versions_from_env(monkeypatch):
    monkeypatch.setenv("TORCH_METAL_VERSION", "1.2.3")
    monkeypatch.setenv("TORCHVISION_METAL_VERSION", "4.5.6")
    cfg_mod = types.SimpleNamespace(load_config=lambda: {})
    sys.modules["solhunter_zero.config"] = cfg_mod
    device_mod = importlib.reload(importlib.import_module("solhunter_zero.device"))
    assert device_mod.load_torch_metal_versions() == ("1.2.3", "4.5.6")


def test_load_versions_from_config(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        """[torch]\n"""
        "torch_metal_version = \"1.2.3\"\n"
        "torchvision_metal_version = \"4.5.6\"\n"
    )

    monkeypatch.delenv("TORCH_METAL_VERSION", raising=False)
    monkeypatch.delenv("TORCHVISION_METAL_VERSION", raising=False)

    def load_config():
        with open(cfg_path, "rb") as fh:
            return tomllib.load(fh)

    cfg_mod = types.SimpleNamespace(load_config=load_config, find_config_file=lambda: str(cfg_path))
    sys.modules["solhunter_zero.config"] = cfg_mod

    device_mod = importlib.reload(importlib.import_module("solhunter_zero.device"))
    assert device_mod.load_torch_metal_versions() == ("1.2.3", "4.5.6")


def test_load_versions_missing(monkeypatch, caplog):
    monkeypatch.delenv("TORCH_METAL_VERSION", raising=False)
    monkeypatch.delenv("TORCHVISION_METAL_VERSION", raising=False)
    cfg_mod = types.SimpleNamespace(load_config=lambda: {})
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", cfg_mod)
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.macos_setup",
        types.SimpleNamespace(_resolve_metal_versions=lambda: ("5.5", "6.6")),
    )
    device_mod = importlib.reload(importlib.import_module("solhunter_zero.device"))
    with caplog.at_level("WARNING"):
        assert device_mod.load_torch_metal_versions() == ("5.5", "6.6")
    assert "Torch Metal versions not specified" in caplog.text


def test_setup_writes_versions_when_missing(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.toml"
    env_path = tmp_path / ".env"

    monkeypatch.setenv("TORCH_METAL_VERSION", "0")
    monkeypatch.setenv("TORCHVISION_METAL_VERSION", "0")

    def load_config():
        if cfg_path.exists() and cfg_path.stat().st_size:
            with open(cfg_path, "rb") as fh:
                return tomllib.load(fh)
        return {}

    cfg_mod = types.SimpleNamespace(load_config=load_config, find_config_file=lambda: str(cfg_path))
    sys.modules["solhunter_zero.config"] = cfg_mod

    device_mod = importlib.reload(importlib.import_module("solhunter_zero.device"))
    monkeypatch.delenv("TORCH_METAL_VERSION", raising=False)
    monkeypatch.delenv("TORCHVISION_METAL_VERSION", raising=False)
    monkeypatch.setattr(
        device_mod,
        "load_torch_metal_versions",
        lambda: (_ for _ in ()).throw(RuntimeError("missing")),
    )

    from pathlib import Path

    sys.modules["tomli_w"] = types.SimpleNamespace(
        dump=lambda cfg, f: f.write(
            (
                "[torch]\n"
                f'torch_metal_version = "{cfg["torch"]["torch_metal_version"]}"\n'
                f'torchvision_metal_version = "{cfg["torch"]["torchvision_metal_version"]}"\n'
            ).encode()
        )
    )
    sys.modules["solhunter_zero.bootstrap_utils"] = types.SimpleNamespace(ensure_deps=lambda: None)
    sys.modules["solhunter_zero.cache_paths"] = types.SimpleNamespace(
        MAC_SETUP_MARKER=Path("marker"), TOOLS_OK_MARKER=Path("marker")
    )
    sys.modules["solhunter_zero.logging_utils"] = types.SimpleNamespace(log_startup=lambda *a, **k: None)
    sys.modules["solhunter_zero.paths"] = types.SimpleNamespace(ROOT=tmp_path)

    ms_mod = importlib.import_module("solhunter_zero.macos_setup")
    monkeypatch.setattr(ms_mod, "CONFIG_PATH", cfg_path)
    monkeypatch.setattr(ms_mod, "_resolve_metal_versions", lambda: ("9.9", "8.8"))

    try:
        ms_mod.TORCH_METAL_VERSION, ms_mod.TORCHVISION_METAL_VERSION = device_mod.load_torch_metal_versions()
    except Exception:
        ms_mod.TORCH_METAL_VERSION, ms_mod.TORCHVISION_METAL_VERSION = ms_mod._resolve_metal_versions()
        ms_mod._write_versions_to_config(
            ms_mod.TORCH_METAL_VERSION, ms_mod.TORCHVISION_METAL_VERSION
        )
        env_lines = []
        for prefix, value in (
            ("TORCH_METAL_VERSION", ms_mod.TORCH_METAL_VERSION),
            ("TORCHVISION_METAL_VERSION", ms_mod.TORCHVISION_METAL_VERSION),
        ):
            env_lines.append(f"{prefix}={value}\n")
        env_path.write_text("".join(env_lines))

    text = cfg_path.read_text()
    env_text = env_path.read_text()
    assert "torch_metal_version = \"9.9\"" in text
    assert "torchvision_metal_version = \"8.8\"" in text
    assert "TORCH_METAL_VERSION=9.9" in env_text
    assert "TORCHVISION_METAL_VERSION=8.8" in env_text
