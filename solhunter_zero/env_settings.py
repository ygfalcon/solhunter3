"""Central helpers for reading API keys and endpoint URLs from the environment."""

from __future__ import annotations

import os
from typing import Optional

from .env_defaults import DEFAULTS

__all__ = [
    "env_value",
    "api_url",
    "api_key",
    "require_env",
]


def env_value(
    name: str,
    *,
    default: Optional[str] = None,
    strip: bool = False,
) -> str:
    """Return the environment value for *name*.

    Parameters
    ----------
    name:
        Environment variable to read.
    default:
        Optional fallback value when the variable is unset.  If omitted we use
        :mod:`solhunter_zero.env_defaults` as the single source of truth.
    strip:
        Whether to ``str.strip`` the resulting value.
    """

    if default is None:
        default = DEFAULTS.get(name)
    value = os.getenv(name, default or "")
    return value.strip() if strip else value


def api_url(name: str, *, default: Optional[str] = None) -> str:
    """Shortcut for ``env_value`` with ``strip=True``."""

    return env_value(name, default=default, strip=True)


def api_key(name: str) -> str:
    """Return the API key stored in ``name`` stripped of whitespace."""

    return env_value(name, strip=True)


def require_env(name: str, *, default: Optional[str] = None) -> str:
    """Return the value of *name* or raise ``RuntimeError`` if empty."""

    value = env_value(name, default=default, strip=True)
    if not value:
        raise RuntimeError(f"Environment variable {name} must be configured")
    return value

