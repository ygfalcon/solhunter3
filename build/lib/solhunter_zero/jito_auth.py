from __future__ import annotations

"""Helpers for acquiring Jito authentication tokens."""

import base64
import json
import os
from pathlib import Path
import urllib.request

from .optional_imports import try_import
from .logging_utils import log_startup

wallet = try_import("solhunter_zero.wallet", stub=None)

AUTH_URL = "https://searcher.jito.network/api/v1/auth"


def fetch_jito_auth() -> str:
    """Retrieve a Jito auth token for the active wallet."""
    select = getattr(wallet, "select_active_keypair", None)
    if callable(select):  # pragma: no cover - runtime fallback
        info = select()
    else:
        from .config_utils import select_active_keypair

        info = select_active_keypair()

    kp_path = Path(wallet.KEYPAIR_DIR) / f"{info.name}.json"
    kp = wallet.load_keypair(str(kp_path))
    pubkey = str(kp.pubkey())

    data = json.dumps({"pubkey": pubkey}).encode()
    req = urllib.request.Request(
        AUTH_URL, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 - external endpoint
        challenge_data = json.loads(resp.read())
    challenge = challenge_data.get("challenge") or challenge_data.get("message") or ""
    if isinstance(challenge, str):
        challenge_bytes = challenge.encode()
    else:
        challenge_bytes = bytes(challenge)

    signature = kp.sign_message(challenge_bytes)
    sig_b64 = base64.b64encode(bytes(signature)).decode()

    data = json.dumps({"pubkey": pubkey, "signature": sig_b64}).encode()
    req = urllib.request.Request(
        AUTH_URL, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 - external endpoint
        token_data = json.loads(resp.read())
    return token_data.get("token", "")


def ensure_jito_auth(env_file: Path) -> None:
    """Ensure Jito auth tokens are present in ``os.environ`` and ``env_file``."""
    auth = os.getenv("JITO_AUTH")
    ws_auth = os.getenv("JITO_WS_AUTH")
    missing = [
        name
        for name, val in (("JITO_AUTH", auth), ("JITO_WS_AUTH", ws_auth))
        if not val
    ]
    if not missing:
        return

    try:
        token = fetch_jito_auth()
    except Exception as exc:  # pragma: no cover - network or wallet failure
        log_startup(f"Failed to obtain Jito auth token: {exc}")
        return

    os.environ["JITO_AUTH"] = token
    os.environ["JITO_WS_AUTH"] = token

    try:
        with env_file.open("a", encoding="utf-8") as fh:
            for name in missing:
                fh.write(f"{name}={token}\n")
        log_startup("Obtained Jito auth token")
    except Exception as exc:  # pragma: no cover - file write failure
        log_startup(f"Failed to update {env_file}: {exc}")
