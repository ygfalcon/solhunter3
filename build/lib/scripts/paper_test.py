from __future__ import annotations

"""Run the paper trading smoke test.

The script invokes :func:`paper.run` with the ``--test`` flag, which fetches a
small slice of live market data and exercises the live trading path in dry-run
mode.  Reports are written to the specified directory (``reports/`` by default)
and the script exits with a non-zero status on failure.
"""

import argparse
import json
import os
import sys
import types
import importlib.machinery
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:  # pragma: no cover - simple path fix
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("SOLHUNTER_TESTING", "1")
try:  # pragma: no cover - ensure stubs are installed
    import sitecustomize  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover - ignore missing module
    pass
else:
    try:  # pragma: no cover - install broader test stubs
        from tests import stubs as _stubs
        sys.modules.pop("sqlalchemy", None)
        _stubs.install_stubs()
    except Exception:  # pragma: no cover - ignore stub errors
        pass

try:  # pragma: no cover - provide minimal solders stub
    import solders.keypair  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    solders = types.ModuleType("solders")
    solders.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    keypair_mod = types.ModuleType("solders.keypair")
    keypair_mod.__spec__ = importlib.machinery.ModuleSpec("solders.keypair", None)

    class Keypair:
        def __init__(self, data: bytes | None = None) -> None:
            self._data = data or bytes(64)

        @classmethod
        def from_bytes(cls, data: bytes) -> "Keypair":
            return cls(data)

        @staticmethod
        def random() -> "Keypair":
            return Keypair()

        def to_bytes(self) -> bytes:
            return self._data

        def to_bytes_array(self) -> list[int]:
            return list(self._data)

    keypair_mod.Keypair = Keypair
    solders.keypair = keypair_mod
    sys.modules.setdefault("solders", solders)
    sys.modules.setdefault("solders.keypair", keypair_mod)

try:  # pragma: no cover - provide minimal cryptography stub
    import cryptography.fernet  # type: ignore  # noqa: F401
except Exception:  # pragma: no cover
    crypto = types.ModuleType("cryptography")
    crypto.__spec__ = importlib.machinery.ModuleSpec("cryptography", None)
    fernet_mod = types.ModuleType("cryptography.fernet")
    fernet_mod.__spec__ = importlib.machinery.ModuleSpec("cryptography.fernet", None)

    class InvalidToken(Exception):
        pass

    class Fernet:
        def __init__(self, key: bytes):
            self._key = key

        def encrypt(self, data: bytes) -> bytes:
            return data

        def decrypt(self, token: bytes) -> bytes:
            return token

    fernet_mod.Fernet = Fernet
    fernet_mod.InvalidToken = InvalidToken
    crypto.fernet = fernet_mod
    sys.modules.setdefault("cryptography", crypto)
    sys.modules.setdefault("cryptography.fernet", fernet_mod)

import paper  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run paper trading smoke test"
    )
    parser.add_argument(
        "--reports",
        type=Path,
        default=Path("reports"),
        help="Directory to write reports",
    )
    args = parser.parse_args(argv)

    try:
        paper.run(["--test", "--reports", str(args.reports)])
    except Exception as exc:  # pragma: no cover - exercised in tests
        print(f"paper test failed: {exc}", file=sys.stderr)
        return 1

    trade_path = args.reports / "trade_history.json"
    if not trade_path.exists():
        print(
            "paper test failed: trade_history.json not produced",
            file=sys.stderr,
        )
        return 1

    try:
        trades = json.loads(trade_path.read_text())
    except Exception as exc:
        print(
            f"paper test failed: cannot load trade history: {exc}",
            file=sys.stderr,
        )
        return 1

    if len(trades) < 2:
        print("paper test failed: insufficient trades", file=sys.stderr)
        return 1

    try:
        buy_total = sum(
            float(t.get("price", 0.0)) * float(t.get("amount", 1.0))
            for t in trades
            if str(t.get("side") or t.get("action")) == "buy"
        )
        sell_total = sum(
            float(t.get("price", 0.0)) * float(t.get("amount", 1.0))
            for t in trades
            if str(t.get("side") or t.get("action")) == "sell"
        )
        if buy_total <= 0:
            raise ValueError("buy total is zero")
        roi = (sell_total - buy_total) / buy_total
    except Exception as exc:
        print(f"paper test failed: cannot compute ROI: {exc}", file=sys.stderr)
        return 1

    print(f"ROI: {roi:.6f}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
