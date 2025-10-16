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

from typing import Any, Dict, Iterable, List, Optional
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

    container: Dict[str, Any] = {}
    for attr in ("value", "result"):
        try:
            attr_value = getattr(obj, attr)
        except Exception:
            continue
        if isinstance(attr_value, (dict, list)):
            container[attr] = attr_value
    if container:
        return container

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


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text, 0)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return None
    return None


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def extract_value_list(resp: Any) -> List[Dict[str, Any]]:
    """Return a list of dictionaries representing ``resp``'s value payload."""

    base = _json_like(resp)
    candidates: Any = []

    if isinstance(base, list):
        candidates = base
    elif isinstance(base, dict):
        def _list_from_container(container: Dict[str, Any]) -> Optional[List[Any]]:
            for key in ("items", "tokens", "assets", "value", "accounts"):
                maybe = container.get(key)
                if isinstance(maybe, list):
                    return maybe
            return None

        paths = (
            ("result", "value"),
            ("result", "accounts"),
            ("result", "items"),
            ("result", "tokens"),
            ("result", "assets"),
            ("result",),
            ("value",),
            ("accounts",),
            ("items",),
            ("tokens",),
            ("assets",),
        )

        for path in paths:
            extracted = _extract_path(base, path)
            if isinstance(extracted, list):
                candidates = extracted
                break
            if isinstance(extracted, dict):
                nested = _list_from_container(extracted)
                if nested is not None:
                    candidates = nested
                    break
        if not candidates:
            nested = _list_from_container(base)
            if nested is not None:
                candidates = nested
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
        if not isinstance(raw, dict):
            continue
        entry: Dict[str, Any] = {}
        for src, dest in (
            ("signature", "signature"),
            ("slot", "slot"),
            ("err", "err"),
            ("memo", "memo"),
            ("confirmationStatus", "confirmationStatus"),
            ("confirmation_status", "confirmationStatus"),
            ("amount", "amount"),
        ):
            if src in raw and raw[src] is not None:
                entry[dest] = raw[src]
        if "slot" in entry:
            entry["slot"] = _as_int(entry["slot"])
        block_time_present = False
        block_time = None
        if "blockTime" in raw:
            block_time = raw.get("blockTime")
            block_time_present = True
        elif "block_time" in raw:
            block_time = raw.get("block_time")
            block_time_present = True
        if block_time_present:
            entry["blockTime"] = _as_int(block_time)
        if "amount" in entry:
            value = entry["amount"]
            if isinstance(value, str):
                coerced = _as_float(value)
                if coerced is not None:
                    entry["amount"] = coerced
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
        if "decimals" in account:
            coerced = _as_int(account["decimals"])
            if coerced is not None:
                account["decimals"] = coerced
        if "uiAmount" in account:
            coerced = _as_float(account["uiAmount"])
            if coerced is not None:
                account["uiAmount"] = coerced
        if "amount" in account and isinstance(account["amount"], str):
            coerced_amount = _as_float(account["amount"])
            if coerced_amount is not None:
                account["amountNumeric"] = coerced_amount
        if account:
            normalised.append(account)
    return normalised


def extract_program_accounts(resp: Any) -> List[Dict[str, Any]]:
    """Return normalised entries for ``get_program_accounts`` calls."""

    return extract_value_list(resp)

