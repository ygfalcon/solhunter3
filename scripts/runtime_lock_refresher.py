#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sys
import time
from typing import Optional

try:
    import redis  # type: ignore
except Exception as exc:  # pragma: no cover - dependency issue
    print(f"[runtime_lock_refresher] redis import failed: {exc}", file=sys.stderr)
    sys.exit(0)


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def _log(level: str, message: str) -> None:
    print(f"[{_timestamp()}] [launch_live][lock][{level}] {message}", flush=True)


REFRESH_INTERVAL = float(os.environ.get("RUNTIME_LOCK_REFRESH_INTERVAL", "20"))
try:
    ttl_seconds = int(os.environ.get("RUNTIME_LOCK_TTL_SECONDS", "60"))
except ValueError:
    ttl_seconds = 60
if ttl_seconds <= 0:
    ttl_seconds = 60
if REFRESH_INTERVAL <= 0:
    REFRESH_INTERVAL = 20.0

KEY = os.environ.get("RUNTIME_LOCK_KEY")
TOKEN = os.environ.get("RUNTIME_LOCK_TOKEN")
REDIS_URL = os.environ.get("REDIS_URL") or "redis://localhost:6379/1"

if not KEY or not TOKEN:
    sys.exit(0)

client = redis.Redis.from_url(REDIS_URL, socket_timeout=1.0)

REFRESH_SCRIPT = """
local current = redis.call('get', KEYS[1])
if not current then
  return -2
end
local ok, data = pcall(cjson.decode, current)
if not ok then
  return -4
end
if data['token'] ~= ARGV[1] then
  return -3
end
data['ts'] = tonumber(ARGV[2])
redis.call('set', KEYS[1], cjson.encode(data), 'EX', tonumber(ARGV[3]))
return redis.call('ttl', KEYS[1])
"""

running = True


def _handle_signal(signum: int, frame) -> None:  # type: ignore[override]
    global running
    running = False


def _refresh() -> Optional[int]:
    try:
        ttl = client.eval(REFRESH_SCRIPT, 1, KEY, TOKEN, str(ttl_seconds), str(time.time()))
    except Exception as exc:  # pragma: no cover - transient errors
        _log("warn", f"Runtime lock refresh failed: {exc}")
        return None
    if ttl is None:
        return None
    if isinstance(ttl, bytes):
        try:
            ttl = int(ttl.decode("utf-8"))
        except Exception:
            ttl = -1
    if not isinstance(ttl, (int, float)):
        ttl = -1
    ttl_int = int(ttl)
    if ttl_int < 0:
        return ttl_int
    _log("info", f"Refreshed runtime lock TTL to {ttl_int}s (interval {REFRESH_INTERVAL}s)")
    if ttl_int <= REFRESH_INTERVAL:
        _log("warn", f"Runtime lock TTL below refresh interval (ttl={ttl_int}s interval={REFRESH_INTERVAL}s)")
    return ttl_int


def main() -> None:
    global running
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    next_run = 0.0
    while running:
        now = time.monotonic()
        if now < next_run:
            time.sleep(min(next_run - now, REFRESH_INTERVAL))
            continue
        ttl_value = _refresh()
        if ttl_value is None:
            time.sleep(min(REFRESH_INTERVAL, 1.0))
            continue
        if ttl_value < 0:
            if ttl_value == -2:
                _log("warn", "Runtime lock missing; stopping refresher")
            elif ttl_value == -3:
                _log("warn", "Runtime lock token mismatch; stopping refresher")
            else:
                _log("warn", f"Runtime lock refresh returned error (code={ttl_value})")
            break
        next_run = time.monotonic() + REFRESH_INTERVAL
    _log("info", "Runtime lock refresher exiting")


if __name__ == "__main__":
    main()
