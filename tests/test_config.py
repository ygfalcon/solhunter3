import os
from pathlib import Path
from solhunter_zero.config import (
    load_config,
    apply_env_overrides,
    set_env_from_config,
    load_dex_config,
    save_config,
    validate_config,
    find_config_file,
)
from solhunter_zero.event_bus import subscribe
import subprocess
import sys
import json
import pytest
from pathlib import Path
import types
from solhunter_zero.jsonutil import dumps


def test_load_config_yaml(tmp_path):
    path = tmp_path / "my.yaml"
    path.write_text(
        "birdeye_api_key: KEY\n"
        "solana_rpc_url: https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "dex_base_url: https://swap.helius.dev\n"
        "agents: [sim]\n"
        "agent_weights:\n  sim: 1.0\n"
        "event_bus_url: ws://bus\n"
    )
    cfg = load_config(str(path))
    assert cfg["birdeye_api_key"] == "KEY"
    assert str(cfg["solana_rpc_url"]).rstrip("/") == "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    assert str(cfg["dex_base_url"]).rstrip("/") == "https://swap.helius.dev"
    assert cfg["agents"] == ["sim"]
    assert cfg["agent_weights"] == {"sim": 1.0}
    assert cfg["event_bus_url"] == "ws://bus"


def test_load_config_toml(tmp_path):
    path = tmp_path / "my.toml"
    path.write_text(
        'birdeye_api_key="KEY"\n'
        'solana_rpc_url="https://mainnet.helius-rpc.com/?api-key=demo-helius-key"\n'
        'dex_base_url="https://swap.helius.dev"\n'
        'event_bus_url="ws://bus"\n'
        'agents=["sim"]\n'
        '[agent_weights]\n'
        'sim=1.0\n'
    )
    cfg = load_config(str(path))
    assert cfg["birdeye_api_key"] == "KEY"
    assert str(cfg["solana_rpc_url"]).rstrip("/") == "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    assert str(cfg["dex_base_url"]).rstrip("/") == "https://swap.helius.dev"
    assert cfg["agents"] == ["sim"]
    assert cfg["event_bus_url"] == "ws://bus"
    assert cfg["agent_weights"] == {"sim": 1.0}


def test_load_config_agents(tmp_path):
    path = tmp_path / "agents.toml"
    path.write_text(
        'solana_rpc_url="https://mainnet.helius-rpc.com/?api-key=demo-helius-key"\n'
        'dex_base_url="https://swap.helius.dev"\n'
        'agents=["sim","exit"]\n[agent_weights]\nsim=0.5\nexit=1.0\n'
    )
    cfg = load_config(str(path))
    assert cfg["agents"] == ["sim", "exit"]
    assert cfg["agent_weights"] == {"sim": 0.5, "exit": 1.0}


def test_env_var_overrides_default_search(tmp_path, monkeypatch):
    default = tmp_path / "config.yaml"
    default.write_text(
        "birdeye_api_key: DEFAULT\n"
        "solana_rpc_url: https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "dex_base_url: https://swap.helius.dev\n"
        "agents: [sim]\n"
        "agent_weights:\n  sim: 1.0\n"
    )
    override = tmp_path / "ov.toml"
    override.write_text(
        'birdeye_api_key="OVR"\n'
        'solana_rpc_url="https://mainnet.helius-rpc.com/?api-key=demo-helius-key"\n'
        'dex_base_url="https://swap.helius.dev"\n'
        'agents=["sim"]\n'
        '[agent_weights]\n'
        'sim=1.0\n'
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SOLHUNTER_CONFIG", str(override))
    cfg = load_config(find_config_file())
    assert cfg["birdeye_api_key"] == "OVR"


def test_find_config_file_order(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("")
    (tmp_path / "config.toml").write_text("")
    assert Path(find_config_file()).resolve() == (tmp_path / "config.toml").resolve()
    (tmp_path / "config.toml").unlink()
    assert Path(find_config_file()).resolve() == (tmp_path / "config.yaml").resolve()


def test_load_config_from_repo_root_when_installed():
    repo_root = Path(__file__).resolve().parents[1]
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", str(repo_root), "--no-deps", "--quiet"]
    )
    code = (
        "import sys, json, pathlib, types;"
        "repo=pathlib.Path().resolve();"
        "sys.path=[p for p in sys.path if pathlib.Path(p).resolve()!=repo];"
        "event_pb2=types.ModuleType('event_pb2');"
        "sys.modules['event_pb2']=event_pb2;"
        "sys.modules['solhunter_zero.event_pb2']=event_pb2;"
        "from solhunter_zero import paths;"
        "from solhunter_zero.config import load_config, find_config_file;"
        "print(paths.ROOT);"
        "cfg=find_config_file();"
        "print(cfg);"
        "print(json.dumps(load_config()));"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, cwd=repo_root
    )
    assert result.returncode == 0
    lines = result.stdout.strip().splitlines()
    assert Path(lines[0]).resolve() != repo_root.resolve()
    assert Path(lines[1]).resolve() == repo_root / "config.toml"
    cfg = json.loads(lines[2])
    assert cfg["solana_rpc_url"].startswith("https://mainnet.helius-rpc.com/?api-key=demo-helius-key")


def test_apply_env_overrides(monkeypatch):
    cfg = {
        "birdeye_api_key": "a",
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
        "risk_tolerance": 0.1,
        "token_suffix": "",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
        "event_bus_url": "ws://old",
    }
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)
    monkeypatch.setenv("BIRDEYE_API_KEY", "NEW")
    monkeypatch.setenv("RISK_TOLERANCE", "0.2")
    monkeypatch.setenv("TOKEN_SUFFIX", "doge")
    monkeypatch.setenv("AGENTS", dumps(["x", "y"]))
    monkeypatch.setenv("AGENT_WEIGHTS", dumps({"x": 1, "y": 1}))
    monkeypatch.setenv("EVENT_BUS_URL", "ws://new")
    result = apply_env_overrides(cfg)
    assert result["birdeye_api_key"] == "NEW"
    assert str(result["solana_rpc_url"]).rstrip("/") == "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    assert result["risk_tolerance"] == "0.2"
    assert result["token_suffix"] == "doge"
    assert result["agents"] == ["x", "y"]
    assert result["agent_weights"] == {"x": 1, "y": 1}
    assert result["event_bus_url"] == "ws://new"


def test_apply_env_overrides_invalid_values(monkeypatch):
    cfg = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
    }
    monkeypatch.setenv("AGENTS", "x,y")
    monkeypatch.setenv("AGENT_WEIGHTS", "not a dict")
    with pytest.raises(ValueError):
        apply_env_overrides(cfg)


def test_llm_env_overrides(monkeypatch):
    cfg = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
        "llm_model": "orig",
        "llm_context_length": 100,
    }
    monkeypatch.setenv("LLM_MODEL", "gpt4")
    monkeypatch.setenv("LLM_CONTEXT_LENGTH", "256")
    result = apply_env_overrides(cfg)
    assert result["llm_model"] == "gpt4"
    assert result["llm_context_length"] == "256"


def test_jito_env_overrides(monkeypatch):
    cfg = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
        "jito_rpc_url": "a",
        "jito_auth": "b",
        "jito_ws_url": "c",
        "jito_ws_auth": "d",
    }
    monkeypatch.setenv("JITO_RPC_URL", "url")
    monkeypatch.setenv("JITO_AUTH", "tok")
    monkeypatch.setenv("JITO_WS_URL", "ws")
    monkeypatch.setenv("JITO_WS_AUTH", "tok2")
    result = apply_env_overrides(cfg)
    assert result["jito_rpc_url"] == "url"
    assert result["jito_auth"] == "tok"
    assert result["jito_ws_url"] == "ws"
    assert result["jito_ws_auth"] == "tok2"


def test_agents_env_round_trip(monkeypatch):
    agents = ["alpha", "beta"]
    weights = {"alpha": 0.5, "beta": 1.5}
    monkeypatch.setenv("AGENTS", dumps(agents))
    monkeypatch.setenv("AGENT_WEIGHTS", dumps(weights))
    cfg = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
    }
    result = apply_env_overrides(cfg)
    assert result["agents"] == agents
    assert result["agent_weights"] == weights


def test_set_env_from_config(monkeypatch):
    cfg = {
        "birdeye_api_key": "A",
        "solana_rpc_url": "RPC",
        "dex_base_url": "https://swap.helius.dev",
        "risk_tolerance": 0.3,
        "token_suffix": "xyz",
        "agents": ["sim"],
        "agent_weights": {"sim": 1.0},
    }
    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.delenv("RISK_TOLERANCE", raising=False)
    monkeypatch.delenv("TOKEN_SUFFIX", raising=False)
    monkeypatch.delenv("AGENTS", raising=False)
    monkeypatch.delenv("AGENT_WEIGHTS", raising=False)
    monkeypatch.setenv("SOLANA_RPC_URL", "EXIST")
    set_env_from_config(cfg)
    assert os.getenv("BIRDEYE_API_KEY") == "A"
    assert os.getenv("SOLANA_RPC_URL") == "EXIST"
    assert os.getenv("RISK_TOLERANCE") == "0.3"
    assert os.getenv("TOKEN_SUFFIX") == "xyz"
    assert os.getenv("AGENTS") is None
    assert os.getenv("AGENT_WEIGHTS") is None


def test_set_env_from_config_helius(monkeypatch):
    cfg = {
        "helius_rpc_url": "https://mainnet.helius-rpc.com/?api-key=CUSTOM",
        "helius_ws_url": "wss://mainnet.helius-rpc.com/?api-key=CUSTOM",
        "helius_api_keys": ["k1", "k2"],
    }
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)
    monkeypatch.delenv("SOLANA_WS_URL", raising=False)
    monkeypatch.delenv("HELIUS_RPC_URL", raising=False)
    monkeypatch.delenv("HELIUS_WS_URL", raising=False)
    monkeypatch.delenv("HELIUS_API_KEY", raising=False)
    monkeypatch.delenv("HELIUS_API_KEYS", raising=False)
    set_env_from_config(cfg)
    assert os.getenv("HELIUS_RPC_URL") == cfg["helius_rpc_url"]
    assert os.getenv("HELIUS_WS_URL") == cfg["helius_ws_url"]
    assert os.getenv("SOLANA_RPC_URL") == cfg["helius_rpc_url"]
    assert os.getenv("SOLANA_WS_URL") == cfg["helius_ws_url"]
    assert os.getenv("HELIUS_API_KEY") == "CUSTOM"
    assert os.getenv("HELIUS_API_KEYS") == "k1,k2"


def test_set_env_llm(monkeypatch):
    cfg = {"llm_model": "model", "llm_context_length": 64}
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("LLM_CONTEXT_LENGTH", raising=False)
    set_env_from_config(cfg)
    assert os.getenv("LLM_MODEL") == "model"
    assert os.getenv("LLM_CONTEXT_LENGTH") == "64"


def test_set_env_from_config_booleans(monkeypatch):
    cfg = {
        "use_flash_loans": True,
        "use_depth_stream": True,
        "use_depth_feed": True,
        "use_rust_exec": True,
        "use_service_exec": True,
        "use_mev_bundles": True,
    }
    monkeypatch.delenv("USE_FLASH_LOANS", raising=False)
    monkeypatch.delenv("USE_DEPTH_STREAM", raising=False)
    monkeypatch.delenv("USE_DEPTH_FEED", raising=False)
    monkeypatch.delenv("USE_RUST_EXEC", raising=False)
    monkeypatch.delenv("USE_SERVICE_EXEC", raising=False)
    monkeypatch.delenv("USE_MEV_BUNDLES", raising=False)
    set_env_from_config(cfg)
    assert os.getenv("USE_FLASH_LOANS") == "True"
    assert os.getenv("USE_DEPTH_STREAM") == "True"
    assert os.getenv("USE_DEPTH_FEED") == "True"
    assert os.getenv("USE_RUST_EXEC") == "True"
    assert os.getenv("USE_SERVICE_EXEC") == "True"
    assert os.getenv("USE_MEV_BUNDLES") == "True"

def test_load_dex_config_env(monkeypatch):
    monkeypatch.setenv("DEX_BASE_URL", "http://b")
    monkeypatch.setenv("ORCA_DEX_URL", "http://o")
    monkeypatch.setenv("DEX_FEES", '{"jupiter": 0.1}')
    cfg = load_dex_config({})
    assert cfg.base_url.rstrip("/") == "http://b"
    assert cfg.venue_urls["orca"] == "http://o"
    assert cfg.fees["jupiter"] == 0.1


def test_default_helius_priority():
    cfg = load_dex_config()
    assert cfg.swap_priorities, "Expected at least one swap priority"
    first = cfg.swap_priorities[0]
    assert first == "helius"
    assert cfg.swap_urls[first].rstrip("/") == "https://swap.helius.dev"


def test_save_config_emits_event(tmp_path):
    events = []

    def handler(payload):
        events.append(payload)

    unsub = subscribe("config_updated", handler)
    try:
        save_config("test.toml", b"foo='bar'")
    finally:
        unsub()
    assert events and events[0].get("foo") == "bar"


def test_get_event_bus_peers(monkeypatch):
    from solhunter_zero.config import get_event_bus_peers

    monkeypatch.setenv("EVENT_BUS_PEERS", "ws://a, ws://b")
    peers = get_event_bus_peers({})
    assert peers == ["ws://a", "ws://b"]

    monkeypatch.delenv("EVENT_BUS_PEERS", raising=False)
    peers = get_event_bus_peers({"event_bus_peers": ["ws://c"]})
    assert peers == ["ws://c"]


def test_validate_config_ok():
    cfg = {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://swap.helius.dev",
        "agents": ["a", "b"],
        "agent_weights": {"a": 1.0, "b": 2.0},
    }
    validate_config(cfg)


def test_validate_config_missing():
    cfg = {
        "dex_base_url": "https://swap.helius.dev",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
    }
    import pytest

    with pytest.raises(ValueError):
        validate_config(cfg)
