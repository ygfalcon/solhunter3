#!/usr/bin/env python3
"""Interactive helper to create or update config.toml for basic setup."""
from __future__ import annotations

import argparse
import ast
import os
import sys
from pathlib import Path
import tomllib

import tomli_w  # type: ignore

from solhunter_zero.config_schema import validate_config
from solhunter_zero.config_utils import ensure_default_config
from solhunter_zero.event_bus import DEFAULT_WS_URL

CONFIG_PATH = ensure_default_config()


def load_config(auto: bool = False) -> dict:
    if CONFIG_PATH.is_file():
        with CONFIG_PATH.open("rb") as fh:
            return tomllib.load(fh)
    return {}


def save_config(cfg: dict) -> None:
    with CONFIG_PATH.open("wb") as fh:
        fh.write(tomli_w.dumps(cfg).encode("utf-8"))


PROMPTS = [
    ("birdeye_api_key", "BIRDEYE_API_KEY", "BirdEye API key"),
    ("solana_rpc_url", "SOLANA_RPC_URL", "Solana RPC URL"),
    ("dex_base_url", "DEX_BASE_URL", "Base DEX URL (mainnet)"),
    ("dex_testnet_url", "DEX_TESTNET_URL", "DEX testnet URL"),
    ("orca_dex_url", "ORCA_DEX_URL", "Orca DEX URL"),
    ("raydium_dex_url", "RAYDIUM_DEX_URL", "Raydium DEX URL"),
    ("event_bus_url", "EVENT_BUS_URL", "Event bus URL"),
]

DEFAULT_AGENTS = ["simulation"]


# Defaults applied when --auto is used. These point to public endpoints but
# can be overridden in the generated config for custom providers.
AUTO_DEFAULTS = {
    "birdeye_api_key": "",
    "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    "dex_base_url": "https://swap.helius.dev",
    "dex_testnet_url": "https://quote-api.jup.ag",
    "orca_dex_url": "",
    "raydium_dex_url": "",
    "event_bus_url": DEFAULT_WS_URL,
}


def _is_placeholder(value: str | None) -> bool:
    if not value:
        return False
    lower = value.lower()
    if "your_" in lower or "example" in lower:
        return True
    if value.startswith("be_") and all(ch in "xX" for ch in value[3:]):
        return True
    if lower.startswith("bd"):
        return True
    return False


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Interactive helper to create or update config.toml for basic setup."
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Populate missing values with defaults without prompting.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run without prompts; use environment variables and defaults only.",
    )
    args = parser.parse_args(argv)

    if args.non_interactive:
        args.auto = True

    cfg = load_config(args.auto)
    for key in ("agents", "agent_weights"):
        val = cfg.get(key)
        if isinstance(val, str):
            try:
                cfg[key] = ast.literal_eval(val)
            except ValueError:
                pass
    updated = False
    for key, env, desc in PROMPTS:
        val = os.getenv(env) or cfg.get(key)
        if _is_placeholder(val):
            val = ""
        missing = val in (None, "")
        if missing:
            if key in cfg:
                cfg.pop(key, None)
                updated = True
            if args.auto:
                if key in AUTO_DEFAULTS:
                    cfg[key] = AUTO_DEFAULTS[key]
                    updated = True
                continue
            if args.non_interactive:
                continue
            try:
                inp = input(f"Enter {desc}: ").strip()
            except EOFError:
                inp = ""
            if inp:
                cfg[key] = inp
                updated = True
        else:
            if cfg.get(key) != val:
                cfg[key] = val
                updated = True

    if "agents" not in cfg:
        cfg["agents"] = DEFAULT_AGENTS.copy()
        updated = True
    if "agent_weights" not in cfg:
        cfg["agent_weights"] = {a: 1.0 for a in cfg["agents"]}
        updated = True
    else:
        missing = [a for a in cfg.get("agents", []) if a not in cfg["agent_weights"]]
        if missing:
            for m in missing:
                cfg["agent_weights"][m] = 1.0
            updated = True

    torch_cfg = cfg.setdefault("torch", {})
    for key, env in (
        ("torch_metal_version", "TORCH_METAL_VERSION"),
        ("torchvision_metal_version", "TORCHVISION_METAL_VERSION"),
    ):
        val = torch_cfg.get(key)
        if _is_placeholder(val) or not val:
            env_val = os.getenv(env)
            if env_val:
                torch_cfg[key] = env_val
                updated = True

    needs_save = updated or args.auto
    if needs_save:
        try:
            validate_config(cfg)
        except ValueError as exc:  # pragma: no cover - unlikely interactive failure
            print(f"Invalid configuration: {exc}", file=sys.stderr)
            sys.exit(1)
        save_config(cfg)
        print(f"Configuration saved to {CONFIG_PATH}")
    else:
        print("Config already contains required values.")


def run(argv: list[str] | None = None) -> None:
    """Programmatic entry point mirroring :func:`main`."""
    main(argv)


if __name__ == "__main__":
    main()
