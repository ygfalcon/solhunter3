"""Environment helpers for production bootstraps."""
from __future__ import annotations

import json
import logging
import os
import re
import stat
import time
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, MutableMapping, Sequence

from solhunter_zero.paths import ROOT

logger = logging.getLogger(__name__)

_PLACEHOLDER_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"REDACTED", re.IGNORECASE),
    re.compile(r"YOUR[_-]", re.IGNORECASE),
    re.compile(r"example", re.IGNORECASE),
    re.compile(r"^test(_|\b)", re.IGNORECASE),
    re.compile(r"xxxx", re.IGNORECASE),
    re.compile(r"\bempty\b", re.IGNORECASE),
)


@dataclass(frozen=True)
class Provider:
    """Describe a provider and the environment variables it requires."""

    name: str
    env_vars: Sequence[str]
    optional: bool = False


@dataclass(frozen=True)
class ProviderIssue:
    """Record a validation failure for a provider variable."""

    provider: str
    env_var: str
    reason: str


def detect_placeholder(value: str | None) -> str | None:
    """Return the placeholder marker found in *value* if any."""

    if not value:
        return "empty"
    for pattern in _PLACEHOLDER_PATTERNS:
        if pattern.search(value):
            return pattern.pattern
    return None


def _parse_env_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.lower().startswith("export "):
        stripped = stripped[7:].lstrip()
    if "=" not in stripped:
        logger.debug("Skipping malformed env line: %s", stripped)
        return None
    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        logger.debug("Skipping env line with empty key: %s", line.rstrip())
        return None

    # Drop inline comments that are not inside quotes.
    in_single = False
    in_double = False
    escaped = False
    comment_index = None
    for index, char in enumerate(value):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "'" and not in_double:
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == "#" and not in_single and not in_double:
            comment_index = index
            break
    if comment_index is not None:
        value = value[:comment_index].rstrip()

    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1]

    value = os.path.expandvars(value)

    return key, value


def _check_permissions(path: Path) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except FileNotFoundError:
        return
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        logger.warning(
            "Environment file %s is group/world accessible (mode=%o)",
            path,
            mode,
        )


def load_env_file(
    path: Path,
    *,
    overwrite: bool = False,
    env: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Load environment variables from *path* into *env* (defaults to ``os.environ``)."""

    target_env: MutableMapping[str, str]
    if env is not None:
        target_env = env
    else:
        target_env = os.environ
    if not path.exists():
        logger.debug("Environment file %s not found", path)
        return {}
    _check_permissions(path)
    loaded: dict[str, str] = {}
    for line in path.read_text().splitlines():
        parsed = _parse_env_line(line)
        if not parsed:
            continue
        key, value = parsed
        if not overwrite and key in target_env:
            logger.debug("Preserving existing env %s", key)
            continue
        target_env[key] = value
        loaded[key] = value
    logger.info("Loaded %d environment variables from %s", len(loaded), path)
    return loaded


def load_production_env(
    paths: Sequence[Path] | None = None,
    *,
    overwrite: bool = False,
    env: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Load production environment files in priority order.

    Returns a mapping of the variables that were newly loaded (and possibly
    overwritten when ``overwrite=True``).
    """

    if env is not None:
        env_map = env
    else:
        env_map = os.environ
    search_paths: list[Path] = []
    if paths:
        search_paths.extend(Path(p) for p in paths)
    else:
        search_paths.extend(
            [
                ROOT / "configs" / ".env.production",
                ROOT / ".env.production",
                ROOT / "etc" / "solhunter" / "env.production",
            ]
        )
    loaded: dict[str, str] = {}
    for candidate in search_paths:
        loaded.update(load_env_file(candidate, overwrite=overwrite, env=env_map))
    return loaded


def validate_providers(
    providers: Iterable[Provider],
    *,
    env: Mapping[str, str] | None = None,
) -> tuple[list[ProviderIssue], list[ProviderIssue]]:
    """Return ``(missing, placeholders)`` for the configured providers."""

    env_map = dict(env or os.environ)
    missing: list[ProviderIssue] = []
    placeholders: list[ProviderIssue] = []
    for provider in providers:
        for var in provider.env_vars:
            value = env_map.get(var)
            if not value:
                if provider.optional:
                    continue
                missing.append(
                    ProviderIssue(provider=provider.name, env_var=var, reason="missing"),
                )
                continue
            marker = detect_placeholder(value)
            if marker and marker != "empty":
                placeholders.append(
                    ProviderIssue(provider=provider.name, env_var=var, reason=marker),
                )
    return missing, placeholders


def assert_providers_ok(providers: Iterable[Provider], env: Mapping[str, str] | None = None) -> None:
    missing, placeholders = validate_providers(providers, env=env)
    problems = missing + placeholders
    if not problems:
        return
    details = ", ".join(f"{item.provider}:{item.env_var} ({item.reason})" for item in problems)
    raise RuntimeError(f"Provider validation failed: {details}")


def format_configured_providers(
    providers: Iterable[Provider],
    env: Mapping[str, str] | None = None,
) -> str:
    env_map = dict(env or os.environ)
    missing, placeholders = validate_providers(providers, env=env_map)
    problematic = {(p.provider, p.env_var) for p in (*missing, *placeholders)}
    configured: list[str] = []
    for provider in providers:
        issues = [
            item
            for item in problematic
            if item[0] == provider.name and item[1] in provider.env_vars
        ]
        if issues:
            continue
        if all(env_map.get(var) for var in provider.env_vars):
            configured.append(provider.name)
    configured.sort()
    if not configured:
        return "Keys OK: none"
    return "Keys OK: " + ", ".join(configured)


def write_env_manifest(
    output_path: Path,
    providers: Iterable[Provider],
    *,
    env: Mapping[str, str] | None = None,
    source_map: Mapping[str, str] | None = None,
    loaded_at: float | None = None,
) -> Path:
    """Write a redacted manifest describing where secrets originated from."""

    env_map = dict(env or os.environ)
    source_lookup = dict(source_map or {})
    timestamp = loaded_at or time.time()
    entries: list[dict[str, object]] = []
    for provider in providers:
        for var in provider.env_vars:
            value = env_map.get(var)
            if value is None:
                continue
            name_hash = hashlib.sha256(var.encode("utf-8")).hexdigest()
            entries.append(
                {
                    "provider": provider.name,
                    "key_hash": name_hash,
                    "source": source_lookup.get(var, "environment"),
                    "configured": bool(value),
                }
            )
    payload = {
        "generated_at": timestamp,
        "entries": entries,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    logger.info("Wrote environment manifest to %s", output_path)
    return output_path
