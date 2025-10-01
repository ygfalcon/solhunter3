import types
import sys

# Stub dependencies used by quick_setup
config_schema_mod = types.ModuleType("solhunter_zero.config_schema")
config_schema_mod.validate_config = lambda cfg: None
sys.modules["solhunter_zero.config_schema"] = config_schema_mod

config_utils_mod = types.ModuleType("solhunter_zero.config_utils")
config_utils_mod.ensure_default_config = lambda: None
sys.modules["solhunter_zero.config_utils"] = config_utils_mod

from scripts import quick_setup


def test_writes_torch_versions(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.toml"
    monkeypatch.setattr(quick_setup, "CONFIG_PATH", cfg_path)
    monkeypatch.setenv("TORCH_METAL_VERSION", "1.2.3")
    monkeypatch.setenv("TORCHVISION_METAL_VERSION", "4.5.6")

    quick_setup.main(["--non-interactive"])

    cfg_text = cfg_path.read_text()
    assert "torch_metal_version = \"1.2.3\"" in cfg_text
    assert "torchvision_metal_version = \"4.5.6\"" in cfg_text
