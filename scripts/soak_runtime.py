#!/usr/bin/env python3
"""
Paper-safe runtime soak:
- For DURATION seconds, repeatedly:
    * Check RL daemon health (mandatory)
    * Optionally check UI /healthz (CHECK_UI_HEALTH=1)
    * Optionally check Redis TCP reachability (USE_REDIS=1)
    * Periodically hit Solana RPC getLatestBlockhash (if SOLANA_RPC_URL set)
- At end, compute success ratios and fail if below thresholds.

Env (with sensible defaults):
  DURATION_SEC=180
  RL_HEALTH_URL=http://127.0.0.1:7070/health      (mandatory)
  UI_HEALTH_URL=http://127.0.0.1:3000/healthz
  CHECK_UI_HEALTH=1
  USE_REDIS=1
  EVENT_BUS_URL=redis://127.0.0.1:6379/0
  SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=demo-helius-key
  # thresholds (fraction 0..1):
  MIN_OK_RL=0.98
  MIN_OK_UI=0.95
  MIN_OK_REDIS=0.95
  MIN_OK_RPC=0.90
"""
from __future__ import annotations
import os, time, json, socket, contextlib, urllib.request
from typing import Tuple

def envf(name, default):
    try:
        return float(os.getenv(name, default))
    except Exception:
        return float(default)

def http_ok(url: str, timeout: float = 2.0) -> Tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return (200 <= r.status < 500), f"status={r.status}"
    except Exception as e:
        return False, f"error:{e}"

def redis_ok(url: str, timeout: float = 1.5) -> Tuple[bool, str]:
    if not url.startswith("redis://"):
        return False, f"bad_scheme:{url}"
    hostport = url.split("redis://", 1)[1].split("/", 1)[0]
    host, port = (hostport.split(":")[0], int(hostport.split(":")[1])) if ":" in hostport else (hostport, 6379)
    try:
        with contextlib.closing(socket.create_connection((host, port), timeout=timeout)):
            return True, "ok"
    except Exception as e:
        return False, f"error:{e}"

def rpc_blockhash(url: str, timeout: float = 3.0) -> Tuple[bool, str]:
    import urllib.request, json as _json
    req = urllib.request.Request(
        url,
        data=_json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "processed"}],
        }).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if r.status >= 400:
                return False, f"status={r.status}"
            j = _json.loads(r.read().decode("utf-8"))
            ok = "result" in j
            return ok, "ok" if ok else "no_result"
    except Exception as e:
        return False, f"error:{e}"

def main():
    duration = int(os.getenv("DURATION_SEC", "180"))
    rl_url = os.getenv("RL_HEALTH_URL", "http://127.0.0.1:7070/health")
    ui_url = os.getenv("UI_HEALTH_URL", "http://127.0.0.1:3000/healthz")
    check_ui = os.getenv("CHECK_UI_HEALTH", "1") == "1"
    use_redis = os.getenv("USE_REDIS", "1") == "1"
    redis_url = os.getenv("EVENT_BUS_URL", "redis://127.0.0.1:6379/0")
    rpc_url = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=demo-helius-key")

    min_ok_rl = envf("MIN_OK_RL", 0.98)
    min_ok_ui = envf("MIN_OK_UI", 0.95)
    min_ok_redis = envf("MIN_OK_REDIS", 0.95)
    min_ok_rpc = envf("MIN_OK_RPC", 0.90)

    # counters
    total = 0
    rl_ok = ui_ok = red_ok = rpc_okc = 0

    t0 = time.time()
    last_rpc = 0.0
    last_red = 0.0
    while time.time() - t0 < duration:
        total += 1
        ok, _ = http_ok(rl_url)
        rl_ok += 1 if ok else 0

        if check_ui:
            ok, _ = http_ok(ui_url)
            ui_ok += 1 if ok else 0

        now = time.time()
        if use_redis and now - last_red >= 5.0:
            ok, _ = redis_ok(redis_url)
            red_ok += 1 if ok else 0
            last_red = now

        if rpc_url and now - last_rpc >= 10.0:
            ok, _ = rpc_blockhash(rpc_url)
            rpc_okc += 1 if ok else 0
            last_rpc = now

        time.sleep(1.0)

    # compute denominators
    ui_den = total if check_ui else 1
    red_den = max(1, int((time.time() - t0) // 5))
    rpc_den = max(1, int((time.time() - t0) // 10))

    metrics = {
        "duration_sec": duration,
        "rl_ok_ratio": (rl_ok / total) if total else 0.0,
        "ui_ok_ratio": (ui_ok / ui_den) if ui_den else 1.0,
        "redis_ok_ratio": (red_ok / red_den) if use_redis else 1.0,
        "rpc_ok_ratio": (rpc_okc / rpc_den),
        "thresholds": {
            "rl": min_ok_rl,
            "ui": min_ok_ui,
            "redis": min_ok_redis,
            "rpc": min_ok_rpc,
        },
    }
    print(json.dumps({"ok": True, "metrics": metrics}, indent=2))

    failures = []
    if metrics["rl_ok_ratio"] < min_ok_rl:
        failures.append(f"RL {metrics['rl_ok_ratio']:.2%} < {min_ok_rl:.0%}")
    if check_ui and metrics["ui_ok_ratio"] < min_ok_ui:
        failures.append(f"UI {metrics['ui_ok_ratio']:.2%} < {min_ok_ui:.0%}")
    if use_redis and metrics["redis_ok_ratio"] < min_ok_redis:
        failures.append(f"Redis {metrics['redis_ok_ratio']:.2%} < {min_ok_redis:.0%}")
    if metrics["rpc_ok_ratio"] < min_ok_rpc:
        failures.append(f"RPC {metrics['rpc_ok_ratio']:.2%} < {min_ok_rpc:.0%}")

    if failures:
        print(json.dumps({"ok": False, "failures": failures}, indent=2))
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
