"""Utilities for dealing with solders RPC response objects.

These helpers normalise return values from ``solana-py`` which now surface
``solders`` response classes instead of plain dictionaries.  The production
code historically expected ``dict`` responses and therefore used ``.get``
lookups.  When the newer objects are encountered those lookups raise
``AttributeError`` or ``TypeError``.  The functions below coerce the
structures back into dictionary form, preserving both camelCase and
snake_case keys so existing consumers continue to work.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List
import json


def _json_like(obj: Any) -> Any:
    """Best-effort conversion of *obj* into standard ``dict``/``list`` types."""

    if isinstance(obj, (dict, list)):
        return obj

    for attr in ("to_json", "to_json_string"):
        fn = getattr(obj, attr, None)
        if callable(fn):
            try:
                payload = fn()
            except Exception:
                continue
            if isinstance(payload, (dict, list)):
                return payload
            if isinstance(payload, str):
                try:
                    return json.loads(payload)
                except Exception:
                    continue

    asdict = getattr(obj, "_asdict", None)
    if callable(asdict):
        try:
            payload = asdict()
            if isinstance(payload, (dict, list)):
                return payload
        except Exception:
            pass

    # Fall back to attribute introspection; works for solders classes that
    # expose read-only attributes via ``__slots__``.
    data: Dict[str, Any] = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        data[name] = value
    return data


def _extract_path(obj: Any, path: Iterable[str]) -> Any:
    cur = obj
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
    return cur


def extract_value_list(resp: Any) -> List[Dict[str, Any]]:
    """Return a list of dictionaries representing ``resp``'s value payload."""

    base = _json_like(resp)
    candidates: Any = []

    if isinstance(base, list):
        candidates = base
    elif isinstance(base, dict):
        for path in (("result", "value"), ("result",), ("value",)):
            extracted = _extract_path(base, path)
            if isinstance(extracted, list):
                candidates = extracted
                break
    else:
        candidates = []

    entries: List[Dict[str, Any]] = []
    for item in candidates or []:
        normalised = _json_like(item)
        if isinstance(normalised, dict):
            entries.append(normalised)
    return entries


def extract_signature_entries(resp: Any) -> List[Dict[str, Any]]:
    """Normalise ``get_signatures_for_address`` responses into dict entries."""

    entries = extract_value_list(resp)
    normalised: List[Dict[str, Any]] = []
    for raw in entries:
        entry: Dict[str, Any] = {}
        for src, dest in (
            ("signature", "signature"),
            ("slot", "slot"),
            ("err", "err"),
            ("memo", "memo"),
            ("confirmationStatus", "confirmationStatus"),
            ("confirmation_status", "confirmationStatus"),
        ):
            if src in raw and raw[src] is not None:
                entry[dest] = raw[src]
        block_time = raw.get("blockTime")
        if block_time is None:
            block_time = raw.get("block_time")
        if block_time is not None:
            entry["blockTime"] = block_time
        if entry:
            normalised.append(entry)
    return normalised


def extract_token_accounts(resp: Any) -> List[Dict[str, Any]]:
    """Normalise ``get_token_largest_accounts`` responses."""

    entries = extract_value_list(resp)
    normalised: List[Dict[str, Any]] = []
    for raw in entries:
        account: Dict[str, Any] = {}
        for src, dest in (
            ("address", "address"),
            ("amount", "amount"),
            ("decimals", "decimals"),
            ("uiAmount", "uiAmount"),
            ("ui_amount", "uiAmount"),
            ("uiAmountString", "uiAmountString"),
            ("ui_amount_string", "uiAmountString"),
        ):
            if src in raw and raw[src] is not None:
                account[dest] = raw[src]
        if account:
            normalised.append(account)
    return normalised


def extract_program_accounts(resp: Any) -> List[Dict[str, Any]]:
    """Return normalised entries for ``get_program_accounts`` calls."""

    return extract_value_list(resp)

