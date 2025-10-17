from __future__ import annotations

"""Global project path utilities."""

from pathlib import Path

# ─────────────────────────────
# Root path discovery
# ─────────────────────────────

# When imported normally, resolve the repo root as parent of this file's parent.
# If frozen or executed directly, fallback to current working directory.
try:
    ROOT = Path(__file__).resolve().parent.parent
except NameError:
    ROOT = Path.cwd()

# ─────────────────────────────
# Common subpaths (lazy resolved)
# ─────────────────────────────

DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"
LOGS_DIR = ROOT / "logs"

for _p in (DATA_DIR, MODELS_DIR, LOGS_DIR):
    _p.mkdir(exist_ok=True)

__all__ = ["ROOT", "DATA_DIR", "MODELS_DIR", "LOGS_DIR"]
