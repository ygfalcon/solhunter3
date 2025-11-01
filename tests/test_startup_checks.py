import types

from solhunter_zero import config as config_mod
from solhunter_zero import startup_checks


class DummyClientSession:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


async def _ok_probe(*args, **kwargs):
    return True, "ok"


def test_perform_checks_uses_resolved_rpc(monkeypatch, tmp_path):
    rpc_url = "https://custom-rpc.solana.test/"
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        """
solana_rpc_url = "https://custom-rpc.solana.test"
dex_base_url = "https://dex.example"
birdeye_api_key = "override-key"
agents = ["alpha"]
[agent_weights]
alpha = 1.0
""".strip()
    )

    monkeypatch.setenv("SOLHUNTER_CONFIG", str(cfg_path))
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)

    captured = []

    async def fake_health(session, url):
        captured.append(url)
        return True, "rpc ok"

    monkeypatch.setattr(startup_checks, "_json_rpc_health", fake_health)
    monkeypatch.setattr(startup_checks, "_probe_birdeye", _ok_probe)
    monkeypatch.setattr(
        startup_checks,
        "aiohttp",
        types.SimpleNamespace(ClientSession=DummyClientSession),
    )

    old_rpc = startup_checks.SOLANA_RPC_URL
    old_key = startup_checks.BIRDEYE_API_KEY
    try:
        startup_checks.perform_checks(
            load_config=config_mod.load_config,
            apply_env_overrides=config_mod.apply_env_overrides,
        )
        updated_rpc = startup_checks.SOLANA_RPC_URL
        updated_key = startup_checks.BIRDEYE_API_KEY
    finally:
        startup_checks.SOLANA_RPC_URL = old_rpc
        startup_checks.BIRDEYE_API_KEY = old_key

    assert captured and all(url == rpc_url for url in captured)
    assert updated_rpc == rpc_url
    assert updated_key == "override-key"
