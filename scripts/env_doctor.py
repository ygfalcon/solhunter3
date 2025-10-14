#!/usr/bin/env python3
"""Environment doctor for SolHunter readiness checks."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import ssl
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import quote_plus, urlparse
from urllib.request import Request, urlopen

try:  # optional dependency
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover - redis optional
    aioredis = None  # type: ignore


@dataclass
class CheckResult:
    name: str
    ok: bool
    message: str


def _fail(name: str, message: str) -> CheckResult:
    return CheckResult(name=name, ok=False, message=message)


def _ok(name: str, message: str) -> CheckResult:
    return CheckResult(name=name, ok=True, message=message)


def check_env_var(name: str, *, allow_empty: bool = False, forbid_values: Optional[Iterable[str]] = None) -> CheckResult:
    value = os.getenv(name)
    if value is None:
        return _fail(name, "not set")
    if not allow_empty and not value.strip():
        return _fail(name, "empty value")
    if forbid_values:
        forbidden = {v.lower() for v in forbid_values}
        if value.strip().lower() in forbidden:
            return _fail(name, "placeholder value detected")
    return _ok(name, "present")


def check_http_endpoint(name: str, url: str, *, timeout: float = 5.0) -> CheckResult:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return _fail(name, f"unsupported scheme {parsed.scheme!r}")
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - controlled URL
            status = resp.status
    except Exception as exc:  # noqa: BLE001
        return _fail(name, f"request failed: {exc}")
    if status >= 400:
        return _fail(name, f"HTTP {status}")
    return _ok(name, f"reachable (HTTP {status})")


def check_ws_endpoint(name: str, url: str, *, timeout: float = 5.0) -> CheckResult:
    parsed = urlparse(url)
    if parsed.scheme not in {"ws", "wss"}:
        return _fail(name, f"unsupported scheme {parsed.scheme!r}")
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    if not host:
        return _fail(name, "missing hostname")
    try:
        family_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        return _fail(name, f"DNS failure: {exc}")
    err: Optional[Exception] = None
    for family, socktype, proto, _canon, sockaddr in family_info:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(timeout)
                if parsed.scheme == "wss":
                    context = ssl.create_default_context()
                    with context.wrap_socket(sock, server_hostname=host) as secure_sock:
                        secure_sock.connect(sockaddr)
                else:
                    sock.connect(sockaddr)
        except Exception as exc:  # noqa: BLE001
            err = exc
            continue
        else:
            return _ok(name, f"reachable at {host}:{port}")
    return _fail(name, f"connection failed: {err}")


async def check_redis(url: str, *, timeout: float = 5.0) -> CheckResult:
    if aioredis is None:
        return _fail("REDIS_URL", "redis python package not installed")
    try:
        client = aioredis.from_url(url, encoding="utf-8", decode_responses=True, socket_connect_timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        return _fail("REDIS_URL", f"invalid URL: {exc}")
    try:
        await asyncio.wait_for(client.ping(), timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        return _fail("REDIS_URL", f"ping failed: {exc}")
    finally:
        try:
            await client.close()
        except Exception:  # pragma: no cover - best effort cleanup
            pass
    return _ok("REDIS_URL", "ping ok")


def _wallet_check(name: str) -> CheckResult:
    path = os.getenv(name)
    if not path:
        return _fail(name, "not set")
    expanded = os.path.expanduser(path)
    if os.path.exists(expanded):
        return _ok(name, f"path exists ({expanded})")
    return _fail(name, f"path missing ({expanded})")


def _resolve_helius_token() -> Tuple[Optional[str], str]:
    """Return the configured Helius token and the env var used."""

    def _clean(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        stripped = value.strip()
        return stripped or None

    direct = _clean(os.getenv("HELIUS_API_KEY"))
    if direct:
        return direct, "HELIUS_API_KEY"

    keys = _clean(os.getenv("HELIUS_API_KEYS"))
    if keys:
        first = _clean(keys.split(",")[0])
        if first:
            return first, "HELIUS_API_KEYS"

    token = _clean(os.getenv("HELIUS_API_TOKEN"))
    if token:
        return token, "HELIUS_API_TOKEN"

    return None, ""


def check_helius_key() -> CheckResult:
    token, source = _resolve_helius_token()
    if token is None:
        return _fail("HELIUS_API_KEY", "no configured API key")

    lowered = token.lower()
    forbidden = {"replace-with-staging-helius-key", "replace-with-production-helius-key"}
    if lowered in forbidden:
        return _fail("HELIUS_API_KEY", "placeholder value detected")

    suffix = f" via {source}" if source else ""
    if lowered == "skip":
        return _ok("HELIUS_API_KEY", f"probe skip requested{suffix}")

    return _ok("HELIUS_API_KEY", f"configured{suffix}")


def _iter_asset_nodes(node: object) -> Iterator[object]:
    if isinstance(node, list):
        for item in node:
            yield item
    elif isinstance(node, dict):
        for key in ("items", "tokens", "assets"):
            if key in node:
                yield from _iter_asset_nodes(node[key])
        for key in ("result", "data"):
            if key in node:
                yield from _iter_asset_nodes(node[key])


def check_helius_das(*, timeout: float = 5.0) -> CheckResult:
    token, source = _resolve_helius_token()
    if token is None:
        return _fail("HELIUS_API_KEY", "no configured API key")

    if token.lower() == "skip":
        label = source or "configured key"
        return _ok("HELIUS_API_KEY", f"Helius DAS probe skipped ({label}=skip)")

    rpc_base = os.getenv("DAS_RPC_URL") or os.getenv("HELIUS_DAS_RPC_URL") or "https://mainnet.helius-rpc.com"
    rpc_base = rpc_base.rstrip("/")
    if "api-key=" in rpc_base:
        endpoint = rpc_base
    else:
        delimiter = "&" if "?" in rpc_base else "?"
        endpoint = f"{rpc_base}{delimiter}api-key={quote_plus(token)}"

    payload = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": "env-doctor",
            "method": "searchAssets",
            "params": {
                "tokenType": "fungible",
                "page": 1,
                "limit": 1,
                "sortBy": "created",
                "sortDirection": "desc",
            },
        }
    ).encode("utf-8")

    req = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - controlled endpoint
            status = resp.status
            body = resp.read()
    except HTTPError as exc:
        return _fail("HELIUS_API_KEY", f"Helius DAS probe failed: HTTP {exc.code}")
    except Exception as exc:  # noqa: BLE001
        return _fail("HELIUS_API_KEY", f"Helius DAS probe failed: {exc}")

    if status >= 400:
        return _fail("HELIUS_API_KEY", f"Helius DAS probe failed: HTTP {status}")

    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        return _fail("HELIUS_API_KEY", f"Helius DAS probe failed: invalid JSON ({exc})")

    if parsed.get("error"):
        return _fail("HELIUS_API_KEY", f"Helius DAS probe failed: {parsed['error']}")

    assets = list(_iter_asset_nodes(parsed.get("result")))
    if not assets:
        return _fail("HELIUS_API_KEY", "Helius DAS probe returned no assets")

    return _ok(
        "HELIUS_API_KEY",
        f"Helius DAS reachable via {source or 'configured key'} (JSON-RPC HTTP {status})",
    )


async def run_checks() -> List[CheckResult]:
    results: List[CheckResult] = []
    results.append(check_helius_key())
    results.append(check_env_var("SOLANA_RPC_URL"))
    results.append(check_env_var("SOLANA_WS_URL"))
    results.append(check_env_var("REDIS_URL"))
    results.append(_wallet_check("PAPER_WALLET_KEYPATH"))
    results.append(_wallet_check("LIVE_WALLET_KEYPATH"))

    rpc_url = os.getenv("SOLANA_RPC_URL")
    if rpc_url:
        results.append(check_http_endpoint("SOLANA_RPC_URL", rpc_url))
    ws_url = os.getenv("SOLANA_WS_URL")
    if ws_url:
        results.append(check_ws_endpoint("SOLANA_WS_URL", ws_url))

    redis_url = os.getenv("REDIS_URL")
    if redis_url:
        results.append(await check_redis(redis_url))

    if any(os.getenv(name) for name in ("HELIUS_API_KEY", "HELIUS_API_KEYS", "HELIUS_API_TOKEN")):
        results.append(check_helius_das())

    return results


def render(results: Iterable[CheckResult]) -> int:
    failures = 0
    for result in results:
        status = "OK" if result.ok else "FAIL"
        print(f"{result.name}: {status} - {result.message}")
        if not result.ok:
            failures += 1
    return 0 if failures == 0 else 1


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Validate SolHunter environment")
    parser.add_argument("--summary", action="store_true", help="Print only failures")
    args = parser.parse_args(argv)

    results = asyncio.run(run_checks())
    if args.summary:
        results_to_render = [r for r in results if not r.ok]
    else:
        results_to_render = list(results)
    exit_code = render(results_to_render)
    if args.summary and exit_code == 0:
        print("all checks passed")
    return exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
