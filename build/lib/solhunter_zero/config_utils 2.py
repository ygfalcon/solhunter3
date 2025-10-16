"""Helpers for configuration and keypair selection.

This module consolidates common setup tasks used by the command line
utilities.  ``ensure_default_config`` makes sure a configuration file
exists, creating one from the bundled template when necessary.
``select_active_keypair`` guarantees that a wallet keypair is selected,
optionally defaulting to automatic selection when only a single keypair is
available.

Both helpers are thin wrappers around existing functionality in the
codebase but provide a consistent entry point for scripts.

``select_active_keypair`` respects the ``AUTO_SELECT_KEYPAIR`` environment
variable.  When the variable is set to ``"1"`` or when ``auto=True`` is
passed the sole keypair is chosen without prompting.  The command line
interfaces expose flags such as ``--auto`` or ``--one-click`` to set this
variable implicitly.
"""

from __future__ import annotations

import os
from pathlib import Path

from .config_bootstrap import ensure_config as _ensure_config
from . import wallet


def ensure_default_config() -> Path:
    """Ensure a usable configuration file exists and return its path."""

    cfg_file, _ = _ensure_config()
    return cfg_file


def select_active_keypair(auto: bool = True) -> wallet.KeypairInfo:
    """Ensure an active keypair is selected and return its info.

    Parameters
    ----------
    auto:
        When ``True`` (the default) the ``AUTO_SELECT_KEYPAIR`` environment
        variable is set to ``"1"`` if not already defined.  This causes the
        first available keypair to be chosen automatically without
        prompting.  Command line tools typically expose a ``--auto`` or
        ``--one-click`` flag which sets this behaviour.  Setting
        ``AUTO_SELECT_KEYPAIR=0`` forces interactive selection when multiple
        keypairs exist.
    """

    if auto and os.getenv("AUTO_SELECT_KEYPAIR") is None:
        os.environ["AUTO_SELECT_KEYPAIR"] = "1"
    return wallet.setup_default_keypair()
