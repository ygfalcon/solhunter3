#!/usr/bin/env bash
set -euo pipefail

# Environment overrides:
#   LAUNCH_LIVE_SKIP_PIP    Skip dependency installation and reuse the existing
#                           virtual environment (useful for pre-provisioned or
#                           fully offline setups).

timestamp() {
  date '+%Y-%m-%dT%H:%M:%S%z'
}

log_info() {
  local msg=$*
  printf '[%s] [launch_live] %s\n' "$(timestamp)" "$msg"
}

log_warn() {
  local msg=$*
  printf '[%s] [launch_live][warn] %s\n' "$(timestamp)" "$msg" >&2
}

detect_pip_online() {
  if [[ -n ${PIP_NO_INDEX:-} ]]; then
    return 1
  fi
  if compgen -G "$ROOT_DIR/requirements*.lock" >/dev/null 2>&1; then
    return 1
  fi
  python3 - <<'PY' >/dev/null 2>&1
import socket
import sys

def check(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False

hosts = [
    ("pypi.org", 443),
    ("files.pythonhosted.org", 443),
]

for host, port in hosts:
    if check(host, port):
        sys.exit(0)

sys.exit(1)
PY
}

EXIT_KEYS=1
EXIT_PREFLIGHT=2
EXIT_CONNECTIVITY=3
EXIT_HEALTH=4
EXIT_DEPS=5
EXIT_SCHEMA=6
EXIT_SOCKET=7

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VENV_DIR="$ROOT_DIR/.venv"
PYTHON_BIN="$VENV_DIR/bin/python3"
PIP_BIN="$VENV_DIR/bin/pip"
DEFAULT_ARTIFACT_ROOT="$ROOT_DIR/artifacts"
DEFAULT_RUN_ID="prelaunch"
export RUNTIME_ARTIFACT_ROOT="${RUNTIME_ARTIFACT_ROOT:-$DEFAULT_ARTIFACT_ROOT}"
export RUNTIME_RUN_ID="${RUNTIME_RUN_ID:-$DEFAULT_RUN_ID}"
# Keep in sync with DEFAULT_RUNTIME_WORKFLOW in
# solhunter_zero/runtime/runtime_wiring.py
DEFAULT_RUNTIME_WORKFLOW="golden-multi-stage-golden-stream"
if [[ -z ${RUNTIME_WORKFLOW:-} ]]; then
  export RUNTIME_WORKFLOW="$DEFAULT_RUNTIME_WORKFLOW"
  log_info "RUNTIME_WORKFLOW defaulted to $RUNTIME_WORKFLOW"
else
  log_info "RUNTIME_WORKFLOW preset to $RUNTIME_WORKFLOW"
fi
if [[ -z ${SOLHUNTER_WORKFLOW:-} ]]; then
  export SOLHUNTER_WORKFLOW="$RUNTIME_WORKFLOW"
fi
ART_DIR="$RUNTIME_ARTIFACT_ROOT/$RUNTIME_RUN_ID"
ARTIFACT_DIR="$ART_DIR"
LOG_DIR="$ARTIFACT_DIR/logs"
mkdir -p "$ARTIFACT_DIR" "$LOG_DIR"
log_info "Runtime artifacts will be written to $ARTIFACT_DIR (logs in $LOG_DIR)"

# Ensure the repository root is always importable when invoking helper scripts.
# "launch_live.sh" may be executed before the package is installed (e.g. from a
# fresh clone), so python invocations need the project root on PYTHONPATH so
# that modules like ``solhunter_zero`` can be imported successfully.
if [[ -n ${PYTHONPATH:-} ]]; then
  export PYTHONPATH="$ROOT_DIR:$PYTHONPATH"
else
  export PYTHONPATH="$ROOT_DIR"
fi

ensure_virtualenv() {
  if [[ ! -x $PYTHON_BIN ]]; then
    log_info "Creating Python virtual environment at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi
  # shellcheck disable=SC1090
  source "$VENV_DIR/bin/activate"
  DEPS_LOG="$LOG_DIR/deps_install.log"
  if [[ -n ${LAUNCH_LIVE_SKIP_PIP:-} ]]; then
    log_info "LAUNCH_LIVE_SKIP_PIP is set; skipping Python dependency installation"
    return
  fi

  local offline_mode=1
  if detect_pip_online; then
    offline_mode=0
  fi

  if (( offline_mode )); then
    log_warn "Offline mode detected; verifying Python dependencies without network access"
    if "$PIP_BIN" check >/dev/null 2>&1; then
      log_info "Python dependencies already satisfied; skipping pip install"
      return
    fi
    log_warn "Dependency check failed; attempting offline installation from cached wheels"
  fi

  local find_links=()
  if (( offline_mode )); then
    local pip_cache_dir
    pip_cache_dir=$("$PIP_BIN" cache dir 2>/dev/null || true)
    if [[ -n $pip_cache_dir ]]; then
      if [[ -d $pip_cache_dir/wheels ]]; then
        find_links+=(--find-links "$pip_cache_dir/wheels")
      elif [[ -d $pip_cache_dir ]]; then
        find_links+=(--find-links "$pip_cache_dir")
      fi
    fi
  fi

  log_info "Installing Python dependencies (see $DEPS_LOG for details)"
  if (( offline_mode )); then
    {
      "$PIP_BIN" install --no-index "${find_links[@]}" -r "$ROOT_DIR/requirements.txt" -r "$ROOT_DIR/requirements-tests.txt" \
        "jsonschema[format-nongpl]==4.23.0"
    } 2>&1 | tee "$DEPS_LOG" >/dev/null
    if [[ ${PIPESTATUS[0]} -ne 0 || ${PIPESTATUS[1]} -ne 0 ]]; then
      log_warn "Failed to install Python dependencies (see $DEPS_LOG)"
      exit $EXIT_DEPS
    fi
    if ! "$PIP_BIN" check >/dev/null 2>&1; then
      log_warn "Offline installation succeeded but dependency check still reports issues"
    fi
    return
  fi

  {
    "$PIP_BIN" install -U pip setuptools wheel &&
      "$PIP_BIN" install -U -r "$ROOT_DIR/requirements.txt" -r "$ROOT_DIR/requirements-tests.txt" \
        "jsonschema[format-nongpl]==4.23.0"
  } 2>&1 | tee "$DEPS_LOG" >/dev/null
  if [[ ${PIPESTATUS[0]} -ne 0 || ${PIPESTATUS[1]} -ne 0 ]]; then
    log_warn "Failed to install Python dependencies (see $DEPS_LOG)"
    exit $EXIT_DEPS
  fi
}

log_python_environment() {
  local interpreter version
  interpreter=$("$PYTHON_BIN" -c 'import sys; print(sys.executable)')
  version=$("$PYTHON_BIN" -c 'import sys; print(f"Python {sys.version.split()[0]}")')
  log_info "Python interpreter: $interpreter"
  log_info "$version"
  log_info "Pinned packages (jsonschema/protobuf/aioredis):"
  "$PIP_BIN" list | grep -E 'jsonschema|protobuf|aioredis' || true
  if [[ $(uname -s) == "Darwin" ]]; then
    export TORCH_METAL_VERSION="${TORCH_METAL_VERSION:-2.8.0}"
    export TORCHVISION_METAL_VERSION="${TORCHVISION_METAL_VERSION:-0.23.0}"
    local metal_report
    metal_report=$("$PYTHON_BIN" - <<'PY'
import json
info = {"torch": None, "torchvision": None, "mps_available": False, "mps_built": False, "error": None}
try:
    import torch  # type: ignore
    info["torch"] = getattr(torch, "__version__", "unknown")
    if hasattr(torch.backends, "mps"):
        try:
            info["mps_available"] = bool(torch.backends.mps.is_available())
            info["mps_built"] = bool(torch.backends.mps.is_built())
        except Exception as exc:  # pragma: no cover - diagnostic only
            info["error"] = f"mps probe failed: {exc}"
except Exception as exc:  # pragma: no cover - torch optional
    info["error"] = str(exc)
try:
    import torchvision  # type: ignore
    info["torchvision"] = getattr(torchvision, "__version__", "unknown")
except Exception:
    pass
print(json.dumps(info))
PY
)
    log_info "Metal detection: env torch=${TORCH_METAL_VERSION:-unset} torchvision=${TORCHVISION_METAL_VERSION:-unset} report=${metal_report}"
  fi
}

run_schema_smoke_tests() {
  log_info "Running golden schema smoke tests"
  if ! "$PYTHON_BIN" -m pytest -q \
    tests/golden_pipeline/test_validation.py::test_schema_validation_smoke \
    tests/golden_pipeline/test_end_to_end_schema.py::test_end_to_end_golden_snapshot_contract; then
    log_warn "Golden schema smoke tests failed"
    echo "Golden schema validation failed; agent suggestions will not appear until schemas are fixed" >&2
    exit $EXIT_SCHEMA
  fi
}

normalize_bus_configuration() {
  "$PYTHON_BIN" - <<'PY'
import json
import os
import socket
import sys
from dataclasses import dataclass
from urllib.parse import urlparse
import shlex

DEFAULT_REDIS = "redis://localhost:6379/1"
DEFAULT_BUS = "ws://127.0.0.1:8779"

channel = os.environ.setdefault("BROKER_CHANNEL", "solhunter-events-v3")
bus_url = os.environ.setdefault("EVENT_BUS_URL", DEFAULT_BUS)

redis_keys = [
    "REDIS_URL",
    "MINT_STREAM_REDIS_URL",
    "MEMPOOL_STREAM_REDIS_URL",
    "AMM_WATCH_REDIS_URL",
]

REDIS_MISMATCH_ERROR = (
    "All Redis URLs must match. Use identical redis://host:port/1 values for "
    "REDIS_URL, MINT_STREAM_REDIS_URL, MEMPOOL_STREAM_REDIS_URL, AMM_WATCH_REDIS_URL, "
    "BROKER_URL, and BROKER_URLS (adjust exports or pass --env with the canonical settings)."
)


@dataclass
class RedisURL:
    scheme: str
    username: str | None
    password: str | None
    host: str
    port: int
    db: int
    query: str


def _record_target(canonical: str) -> None:
    global target_url
    if canonical is None:
        return
    if target_url is None:
        target_url = canonical
    elif canonical != target_url:
        errors.append(REDIS_MISMATCH_ERROR)


def _normalize_single_key(
    key: str, raw: str | None, *, force_db: int | None = None
) -> None:
    if raw is None:
        return
    os.environ[key] = raw
    try:
        parts = _canonical(raw)
    except Exception:
        errors.append(f"{key} has invalid Redis URL: {raw}")
        return
    canonical_db = force_db if force_db is not None else parts.db
    if force_db is None and parts.db != 1:
        errors.append(
            f"{key} targets database {parts.db}; configure database 1 for all runtime services "
            f"(export {key}=redis://localhost:6379/1 or update your env file)."
        )
    canonical = _format_url(parts, db=canonical_db)
    os.environ[key] = canonical
    manifest[key] = canonical
    redacted_manifest[key] = _format_url(parts, db=canonical_db, redact=True)
    _record_target(canonical)


def _normalize_multi_key(
    key: str, raw: str | None, *, force_db: int | None = None
) -> None:
    if not raw:
        return
    os.environ[key] = raw
    parts = [segment.strip() for segment in raw.split(",") if segment.strip()]
    canonical_parts: list[str] = []
    redacted_parts: list[str] = []
    for part in parts:
        try:
            redis_parts = _canonical(part)
        except Exception:
            errors.append(f"{key} has invalid Redis URL: {part}")
            continue
        canonical_db = force_db if force_db is not None else redis_parts.db
        if force_db is None and redis_parts.db != 1:
            errors.append(
                f"{key} targets database {redis_parts.db}; configure database 1 for all runtime services "
                f"(export {key}=redis://localhost:6379/1 or update your env file)."
            )
        canonical = _format_url(redis_parts, db=canonical_db)
        canonical_parts.append(canonical)
        redacted_parts.append(_format_url(redis_parts, db=canonical_db, redact=True))
        _record_target(canonical)
    if canonical_parts:
        canonical_value = ",".join(canonical_parts)
        os.environ[key] = canonical_value
        manifest[key] = canonical_value
        redacted_manifest[key] = ",".join(redacted_parts)
    else:
        os.environ.pop(key, None)

def _canonical(url: str) -> RedisURL:
    candidate = url if "://" in url else f"redis://{url}"
    parsed = urlparse(candidate)
    scheme = parsed.scheme or "redis"
    host = parsed.hostname or "localhost"
    port = parsed.port or 6379
    path = (parsed.path or "/").lstrip("/")
    segment = path.split("/", 1)[0]
    db = int(segment) if segment else 0
    return RedisURL(
        scheme=scheme,
        username=parsed.username,
        password=parsed.password,
        host=host,
        port=port,
        db=db,
        query=parsed.query or "",
    )


def _format_url(parts: RedisURL, *, db: int | None = None, redact: bool = False) -> str:
    target_db = parts.db if db is None else db
    userinfo = _format_userinfo(parts, redact=redact)
    netloc = parts.host
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if userinfo:
        netloc = f"{userinfo}@{netloc}"
    url = f"{parts.scheme}://{netloc}/{target_db}"
    if parts.query:
        url = f"{url}?{parts.query}"
    return url


def _format_userinfo(parts: RedisURL, *, redact: bool) -> str:
    if parts.username is None and parts.password is None:
        return ""
    if redact:
        return "****"
    username = parts.username or ""
    password = parts.password
    if password is None:
        return username
    if username:
        return f"{username}:{password}"
    return f":{password}"


def _redact_url_for_log(value: str) -> str:
    try:
        parsed = urlparse(value)
    except Exception:
        return value
    if not parsed.netloc or "@" not in parsed.netloc:
        return value
    _, _, host_segment = parsed.netloc.rpartition("@")
    redacted_netloc = f"****@{host_segment}"
    return parsed._replace(netloc=redacted_netloc).geturl()

errors: list[str] = []
manifest: dict[str, str] = {}
redacted_manifest: dict[str, str] = {}
target_url: str | None = None

for key in redis_keys:
    raw = os.environ.get(key) or DEFAULT_REDIS
    os.environ[key] = raw
    try:
        parts = _canonical(raw)
    except Exception:
        errors.append(f"{key} has invalid Redis URL: {raw}")
        continue
    if parts.db != 1:
        errors.append(
            f"{key} targets database {parts.db}; configure database 1 for all runtime services "
            "(export {key}=redis://localhost:6379/1 or update your env file)."
        )
    canonical = _format_url(parts)
    os.environ[key] = canonical
    manifest[key] = canonical
    redacted_manifest[key] = _format_url(parts, redact=True)
    _record_target(canonical)

_normalize_single_key("BROKER_URL", os.environ.get("BROKER_URL"), force_db=1)
_normalize_multi_key("BROKER_URLS", os.environ.get("BROKER_URLS"), force_db=1)

channel_keys = [
    "MINT_STREAM_BROKER_CHANNEL",
    "MEMPOOL_STREAM_BROKER_CHANNEL",
    "AMM_WATCH_BROKER_CHANNEL",
]

for key in channel_keys:
    value = os.environ.get(key)
    if value and value != channel:
        errors.append(
            f"{key} is set to {value} but BROKER_CHANNEL={channel}. Use the same channel for all producers."
        )
    else:
        os.environ.setdefault(key, channel)

if errors:
    print("\n".join(dict.fromkeys(errors)), file=sys.stderr)
    sys.exit(1)

manifest_line = (
    "RUNTIME_MANIFEST "
    f"channel={channel} "
    f"redis={redacted_manifest.get('REDIS_URL', _redact_url_for_log(DEFAULT_REDIS))} "
    f"mint_stream={redacted_manifest.get('MINT_STREAM_REDIS_URL', _redact_url_for_log(DEFAULT_REDIS))} "
    f"mempool={redacted_manifest.get('MEMPOOL_STREAM_REDIS_URL', _redact_url_for_log(DEFAULT_REDIS))} "
    f"amm_watch={redacted_manifest.get('AMM_WATCH_REDIS_URL', _redact_url_for_log(DEFAULT_REDIS))} "
    f"bus={_redact_url_for_log(bus_url)}"
)
print(manifest_line, file=sys.stderr)

exports = {
    "BROKER_CHANNEL": channel,
    "EVENT_BUS_URL": bus_url,
    **manifest,
}
for key in ("BROKER_URL", "BROKER_URLS"):
    value = os.environ.get(key)
    if value:
        exports[key] = value
for key in channel_keys:
    exports[key] = os.environ[key]

for key, value in exports.items():
    print(f"export {key}={shlex.quote(value)}")
PY
}

ensure_redis() {
  "$PYTHON_BIN" - <<'PY'
import os
from solhunter_zero.redis_util import ensure_local_redis_if_needed
urls = []
for key in ("REDIS_URL", "BROKER_URL", "BROKER_URLS"):
    raw = os.getenv(key)
    if not raw:
        continue
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    urls.extend(parts)
ensure_local_redis_if_needed(urls)
PY
}

redis_health() {
  if [[ -z ${REDIS_URL:-} ]]; then
    return 0
  fi
  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli -u "$REDIS_URL" PING >/dev/null 2>&1
  else
    "$PYTHON_BIN" - <<'PY'
import os
import asyncio
from solhunter_zero.production import ConnectivityChecker
async def _check():
    checker = ConnectivityChecker()
    try:
        ok = await checker.check("redis")
        if not ok:
            raise SystemExit(1)
    except AttributeError:
        for target in checker.targets:
            if target.get("name") == "redis":
                result = await checker._probe_redis(target["name"], target["url"])
                if not result.ok:
                    raise SystemExit(1)
asyncio.run(_check())
PY
  fi
}

wait_for_socket_release() {
  "$PYTHON_BIN" - <<'PY'
import ipaddress
import os
import socket
import time
from urllib.parse import urlparse

DEFAULT_TIMEOUT = 30.0


def _read_timeout(raw: str | None) -> float:
    if not raw:
        return DEFAULT_TIMEOUT
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT
    return max(value, 0.0)


bus_url = os.environ.get("EVENT_BUS_URL", "ws://127.0.0.1:8779")
parsed = urlparse(bus_url)
host = parsed.hostname or "127.0.0.1"
port = parsed.port or 8779
timeout = _read_timeout(os.environ.get("EVENT_BUS_RELEASE_TIMEOUT"))


def _is_local_host(hostname: str) -> bool:
    normalized = (hostname or "").strip().lower()
    if not normalized or normalized == "localhost":
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return address.is_loopback


if not _is_local_host(host):
    print(f"free {host} {port} remote")
    raise SystemExit(0)


def port_busy() -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except OSError:
        return False


deadline = time.monotonic() + timeout
sleep_interval = 1.0
max_sleep = 5.0

while True:
    if not port_busy():
        print(f"free {host} {port}")
        break

    now = time.monotonic()
    if now >= deadline:
        print(f"busy {host} {port}")
        break

    remaining = max(deadline - now, 0.0)
    time.sleep(min(sleep_interval, remaining))
    sleep_interval = min(sleep_interval * 1.5, max_sleep)
PY
}

wait_for_ui_socket_release() {
  "$PYTHON_BIN" - <<'PY'
import ipaddress
import os
import socket
import time

DEFAULT_TIMEOUT = 30.0


def _read_timeout(raw: str | None) -> float:
    if not raw:
        return DEFAULT_TIMEOUT
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT
    return max(value, 0.0)


def _read_port(raw: str | None) -> int | None:
    if raw is None:
        return None
    candidate = str(raw).strip()
    if not candidate:
        return None
    try:
        port = int(candidate)
    except ValueError:
        return None
    return port if port > 0 else None


def _is_local_host(hostname: str) -> bool:
    normalized = (hostname or "").strip().lower()
    if not normalized or normalized in {"localhost", "127.0.0.1"}:
        return True
    if normalized in {"0.0.0.0", "::"}:
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return address.is_loopback


def _probe_host(hostname: str) -> str:
    normalized = (hostname or "").strip()
    if not normalized or normalized == "0.0.0.0":
        return "127.0.0.1"
    if normalized == "::":
        return "::1"
    return normalized


host = os.environ.get("UI_HOST", "127.0.0.1") or "127.0.0.1"
port = _read_port(os.environ.get("UI_PORT", "5001"))
timeout = _read_timeout(os.environ.get("UI_SOCKET_RELEASE_TIMEOUT"))

if port is None:
    raw_port = os.environ.get("UI_PORT") or "0"
    print(f"free {host} {raw_port} disabled")
    raise SystemExit(0)

if not _is_local_host(host):
    print(f"free {host} {port} remote")
    raise SystemExit(0)


def port_busy() -> bool:
    try:
        with socket.create_connection((_probe_host(host), port), timeout=0.25):
            return True
    except OSError:
        return False


deadline = time.monotonic() + timeout
sleep_interval = 1.0
max_sleep = 5.0

while True:
    if not port_busy():
        print(f"free {host} {port}")
        break

    now = time.monotonic()
    if now >= deadline:
        print(f"busy {host} {port}")
        break

    remaining = max(deadline - now, 0.0)
    time.sleep(min(sleep_interval, remaining))
    sleep_interval = min(sleep_interval * 1.5, max_sleep)
PY
}

acquire_runtime_lock() {
  local output
  output=$("$PYTHON_BIN" - <<'PY'
import json
import os
import socket
import sys
import time
import uuid

try:
    import redis  # type: ignore
except Exception as exc:  # pragma: no cover - dependency issue
    print(f"Unable to import redis client: {exc}", file=sys.stderr)
    sys.exit(1)

redis_url = os.environ.get("REDIS_URL") or "redis://localhost:6379/1"
channel = os.environ.get("BROKER_CHANNEL") or "solhunter-events-v3"
client = redis.Redis.from_url(redis_url, socket_timeout=1.0)
key = f"solhunter:runtime:lock:{channel}"
token = str(uuid.uuid4())
host = socket.gethostname()
pid = os.getpid()
ttl_seconds = 60
payload = {
    "pid": pid,
    "channel": channel,
    "host": host,
    "token": token,
    "ts": time.time(),
}

def _describe(data: dict[str, object]) -> str:
    pid = data.get("pid")
    host = data.get("host") or "unknown"
    return f"pid={pid} host={host}"

existing = client.get(key)
if existing:
    try:
        data = json.loads(existing.decode("utf-8"))
    except Exception:
        data = {"raw": existing.decode("utf-8", "ignore")}
    existing_host = data.get("host")
    existing_pid = data.get("pid")
    same_host = isinstance(existing_host, str) and existing_host == host
    if same_host and isinstance(existing_pid, int):
        try:
            os.kill(existing_pid, 0)
        except Exception:
            pass
        else:
            print(
                "Another runtime is already using this channel (" + _describe(data) + ").",
                file=sys.stderr,
            )
            sys.exit(1)
    if same_host:
        client.delete(key)
        time.sleep(0.2)

if not client.set(key, json.dumps(payload), nx=True, ex=ttl_seconds):
    existing = client.get(key)
    if existing:
        try:
            data = json.loads(existing.decode("utf-8"))
        except Exception:
            data = {"raw": existing.decode("utf-8", "ignore")}
        existing_host = data.get("host")
        existing_pid = data.get("pid")
        same_host = isinstance(existing_host, str) and existing_host == host
        same_pid = isinstance(existing_pid, int) and existing_pid == pid
        if same_host and same_pid:
            token = data.get("token") or token
            payload["token"] = token
            print(f"{key} {token}")
            sys.exit(0)
        if not same_host or not same_pid:
            print(
                "Taking over runtime lock from "
                + _describe(data)
                + f" (current host={host} pid={pid})",
                file=sys.stderr,
            )
            client.set(key, json.dumps(payload), ex=ttl_seconds)
            print(f"{key} {token}")
            sys.exit(0)
        print(
            "Another runtime is already using this channel (" + _describe(data) + ").",
            file=sys.stderr,
        )
    else:
        print("Unable to acquire runtime lock", file=sys.stderr)
    sys.exit(1)

print(f"{key} {token}")
PY
)
  RUNTIME_LOCK_KEY=$(echo "$output" | awk '{print $1}')
  RUNTIME_LOCK_TOKEN=$(echo "$output" | awk '{print $2}')
  if [[ -z ${RUNTIME_LOCK_KEY:-} || -z ${RUNTIME_LOCK_TOKEN:-} ]]; then
    log_warn "Failed to acquire runtime lock"
    exit $EXIT_HEALTH
  fi
  log_info "Acquired runtime lock key=$RUNTIME_LOCK_KEY"
}

release_runtime_lock() {
  if [[ -z ${RUNTIME_LOCK_KEY:-} || -z ${RUNTIME_LOCK_TOKEN:-} ]]; then
    return
  fi
  RUNTIME_LOCK_KEY="$RUNTIME_LOCK_KEY" RUNTIME_LOCK_TOKEN="$RUNTIME_LOCK_TOKEN" \
    "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

try:
    import redis  # type: ignore
except Exception:
    sys.exit(0)

key = os.environ.get("RUNTIME_LOCK_KEY")
token = os.environ.get("RUNTIME_LOCK_TOKEN")
redis_url = os.environ.get("REDIS_URL") or "redis://localhost:6379/1"
if not key or not token:
    sys.exit(0)

client = redis.Redis.from_url(redis_url, socket_timeout=1.0)

release_script = """
local current = redis.call('get', KEYS[1])
if not current then
  return 0
end
local ok, data = pcall(cjson.decode, current)
if not ok then
  return 0
end
if data['token'] == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

try:
    client.eval(release_script, 1, key, token)
except Exception:
    existing = client.get(key)
    if not existing:
        sys.exit(0)
    try:
        data = json.loads(existing.decode("utf-8"))
    except Exception:
        data = {"token": None}
    if data.get("token") == token:
        client.delete(key)
PY
  RUNTIME_LOCK_KEY=""
  RUNTIME_LOCK_TOKEN=""
}

extract_ui_url() {
  "$PYTHON_BIN" - <<'PY'
import json
import os
import re
import sys

log_path = os.environ.get("RUNTIME_LOG_PATH")
if not log_path or not os.path.exists(log_path):
    sys.exit(0)

pattern = re.compile(r"UI_READY .*url=([^ ]+)")
with open(log_path, "r", encoding="utf-8", errors="ignore") as handle:
    for line in handle:
        match = pattern.search(line)
        if match:
            print(match.group(1))
            break
PY
}

check_ui_health() {
  local log=$1
  local url=""
  local http_disabled=0
  local -a disable_reasons=()
  local disable_env=${UI_DISABLE_HTTP_SERVER:-}
  if [[ -n $disable_env ]]; then
    local normalized=${disable_env,,}
    case $normalized in
      1|true|yes)
        http_disabled=1
        disable_reasons+=("UI_DISABLE_HTTP_SERVER=$disable_env")
        ;;
    esac
  fi
  url=$(RUNTIME_LOG_PATH="$log" extract_ui_url)
  if (( http_disabled == 0 )) && [[ -n $url ]]; then
    if [[ $url == "unavailable" ]]; then
      http_disabled=1
      disable_reasons+=("ui_url=unavailable")
    else
      local parsed_port=""
      if parsed_port=$(UI_HEALTH_PARSE_URL="$url" "$PYTHON_BIN" - <<'PY'
import os
import sys
from urllib.parse import urlparse

url = os.environ.get("UI_HEALTH_PARSE_URL")
if not url:
    sys.exit(1)

parsed = urlparse(url)
if parsed.port is None:
    sys.exit(1)

print(parsed.port)
PY
      ); then
        if [[ $parsed_port == "0" ]]; then
          http_disabled=1
          disable_reasons+=("port=0")
        fi
      fi
    fi
  fi
  if (( http_disabled == 1 )); then
    local reason_str
    if (( ${#disable_reasons[@]} > 0 )); then
      local IFS=", "
      reason_str="${disable_reasons[*]}"
    else
      reason_str="http disabled"
    fi
    log_info "UI health check skipped: HTTP server disabled (${reason_str})"
    return 0
  fi
  if [[ -z $url ]]; then
    log_warn "UI readiness URL not yet available for health check"
    return 1
  fi
  local target="${url%/}/ui/meta"
  local total_timeout_raw="${READY_TIMEOUT:-30}"
  local total_timeout=30
  if [[ $total_timeout_raw =~ ^[0-9]+$ ]]; then
    total_timeout=$total_timeout_raw
  fi
  if (( total_timeout <= 0 )); then
    total_timeout=1
  fi
  local deadline=$((SECONDS + total_timeout))
  local sleep_seconds=1
  local max_sleep=10
  local attempt=0
  while (( SECONDS < deadline )); do
    attempt=$((attempt + 1))
    log_info "UI health check attempt ${attempt} GET $target"
    if UI_HEALTH_URL="$target" "$PYTHON_BIN" - <<'PY'
import os
import sys
import urllib.request

url = os.environ.get("UI_HEALTH_URL")
if not url:
    sys.exit(1)

req = urllib.request.Request(url, headers={"User-Agent": "solhunter-launcher"})
with urllib.request.urlopen(req, timeout=5) as resp:
    if getattr(resp, "status", 200) >= 400:
        sys.exit(1)
sys.exit(0)
PY
    then
      log_info "UI health endpoint responded successfully (url=$target)"
      return 0
    fi
    local now=$SECONDS
    if (( now >= deadline )); then
      break
    fi
    local remaining=$((deadline - now))
    local sleep_for=$sleep_seconds
    if (( sleep_for > remaining )); then
      sleep_for=$remaining
    fi
    log_warn "UI health endpoint check failed (attempt ${attempt}); retrying in ${sleep_for}s (remaining ${remaining}s)"
    sleep "$sleep_for"
    if (( sleep_seconds < max_sleep )); then
      sleep_seconds=$((sleep_seconds * 2))
      if (( sleep_seconds > max_sleep )); then
        sleep_seconds=$max_sleep
      fi
    fi
  done
  if (( attempt == 0 )); then
    attempt=1
  fi
  log_warn "UI health endpoint check failed after ${attempt} attempts"
  return 1
}

usage() {
  cat <<'EOF'
Usage: bash scripts/launch_live.sh --env <env-file> --micro <0|1> [--canary] --budget <usd> --risk <ratio> --preflight <runs> --soak <seconds> [--config <path>]
EOF
}

validate_config_path() {
  local candidate=$1
  if [[ -z $candidate ]]; then
    return 0
  fi
  if [[ ! -r $candidate ]]; then
    echo "Config file $candidate must exist and be readable" >&2
    exit $EXIT_KEYS
  fi
}

need_val() {
  # ensure a flag expecting a value actually has one
  if [[ -z ${2:-} || ${2:-} == --* ]]; then
    echo "Flag $1 requires a value" >&2
    usage
    exit 1
  fi
}

ENV_FILE=""
MICRO_FLAG=""
CANARY_MODE=0
CANARY_BUDGET=""
CANARY_RISK=""
PREFLIGHT_RUNS=2
SOAK_DURATION=180
CONFIG_PATH=""
RUNTIME_LOCK_KEY=""
RUNTIME_LOCK_TOKEN=""

declare -a POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --env)
      need_val "$1" "${2:-}"; ENV_FILE=$2
      shift 2
      ;;
    --micro)
      need_val "$1" "${2:-}"; MICRO_FLAG=$2
      shift 2
      ;;
    --canary)
      CANARY_MODE=1
      shift
      ;;
    --budget)
      need_val "$1" "${2:-}"; CANARY_BUDGET=$2
      shift 2
      ;;
    --risk)
      need_val "$1" "${2:-}"; CANARY_RISK=$2
      shift 2
      ;;
    --preflight)
      need_val "$1" "${2:-}"; PREFLIGHT_RUNS=$2
      shift 2
      ;;
    --soak)
      need_val "$1" "${2:-}"; SOAK_DURATION=$2
      shift 2
      ;;
    --config)
      need_val "$1" "${2:-}"; CONFIG_PATH=$2
      validate_config_path "$CONFIG_PATH"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ ${#POSITIONAL[@]} -gt 0 ]]; then
  echo "Unexpected arguments: ${POSITIONAL[*]}" >&2
  usage
  exit 1
fi

if [[ -z $ENV_FILE ]]; then
  echo "--env is required" >&2
  usage
  exit 1
fi

if [[ -z $MICRO_FLAG ]]; then
  echo "--micro is required" >&2
  usage
  exit 1
fi

if [[ $MICRO_FLAG != "0" && $MICRO_FLAG != "1" ]]; then
  echo "--micro must be 0 or 1" >&2
  exit 1
fi

if ! [[ $PREFLIGHT_RUNS =~ ^[0-9]+$ ]]; then
  echo "--preflight must be an integer" >&2
  exit 1
fi

if [[ $PREFLIGHT_RUNS -ne 2 ]]; then
  echo "--preflight must be 2 to cover micro on/off passes" >&2
  exit $EXIT_PREFLIGHT
fi

if [[ ! -f $ENV_FILE ]]; then
  echo "Environment file $ENV_FILE not found" >&2
  exit $EXIT_KEYS
fi

validate_config_path "$CONFIG_PATH"

if [[ $CANARY_MODE -eq 1 ]]; then
  if [[ -z $CANARY_BUDGET || -z $CANARY_RISK ]]; then
    echo "--canary requires --budget and --risk" >&2
    exit 1
  fi
fi

if ! [[ $SOAK_DURATION =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "--soak must be numeric" >&2
  exit 1
fi

log_info "Starting launch_live with env=$ENV_FILE micro=$MICRO_FLAG canary=$CANARY_MODE preflight_runs=$PREFLIGHT_RUNS soak=${SOAK_DURATION}s"

ensure_virtualenv
log_python_environment

MANIFEST_RAW=""
validate_env_file() {
  "$PYTHON_BIN" - "$ENV_FILE" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
if not env_path.exists():
    print(f"Environment file {env_path} missing", file=sys.stderr)
    raise SystemExit(1)
pattern_strings = [
    r"REDACTED",
    r"YOUR[_-]",
    r"\$\{",
    r"EXAMPLE",
    r"XXXX",
    r"CHANGE_ME",
    r"PLACEHOLDER",
]
patterns = [re.compile(pat, re.IGNORECASE) for pat in pattern_strings]
values: dict[str, str] = {}
for idx, raw_line in enumerate(env_path.read_text().splitlines(), start=1):
    line = raw_line.strip()
    if not line or line.startswith('#'):
        continue
    if '=' not in line:
        continue
    key, value = line.split('=', 1)
    key = re.sub(r'^export\s+', '', key.strip())
    value = value.strip().strip('"').strip("'")
    values[key] = value
    if value == "":
        print(f"missing value for {key}", file=sys.stderr)
        raise SystemExit(1)
    for pat in patterns:
        if pat.search(value):
            print(f"placeholder detected for {key}: {pat.pattern}", file=sys.stderr)
            raise SystemExit(1)
manifest = []
for key, value in sorted(values.items()):
    lowered = key.lower()
    if any(tok in lowered for tok in ("secret", "key", "token", "pass", "private", "pwd")):
        masked = "***"
    else:
        masked = value
    manifest.append(f"{key}={masked}")
print("\n".join(manifest))
PY
}

log_info "Validating environment file $ENV_FILE"
if ! MANIFEST_RAW=$(validate_env_file); then
  exit $EXIT_KEYS
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Ensure Jupiter websocket uses the stats endpoint unless explicitly overridden.
export JUPITER_WS_URL="${JUPITER_WS_URL:-wss://stats.jup.ag/ws}"

if [[ -n $CONFIG_PATH ]]; then
  export CONFIG_PATH
fi

PROVIDER_STATUS=""
log_info "Checking configured provider credentials and connectivity"
if ! PROVIDER_STATUS=$("$PYTHON_BIN" - <<'PY'
from solhunter_zero.production import Provider, assert_providers_ok, format_configured_providers
import sys
providers = [
    Provider("Solana", ("SOLANA_RPC_URL", "SOLANA_WS_URL")),
    Provider("Helius", ("HELIUS_API_KEY",)),
    Provider("Redis", ("REDIS_URL",), optional=True),
    Provider("UI", ("UI_WS_URL",), optional=True),
    Provider("Helius-DAS", ("DAS_BASE_URL",), optional=True),
]
try:
    assert_providers_ok(providers)
except Exception as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(1)
print(format_configured_providers(providers))
PY
); then
  exit $EXIT_KEYS
fi

log_info "Environment manifest (sensitive values masked)"
printf '%s\n' "$MANIFEST_RAW"
log_info "Provider status"
printf '%s\n' "$PROVIDER_STATUS"

log_info "Standardising event bus and Redis configuration"
if ! BUS_EXPORTS=$(normalize_bus_configuration); then
  log_warn "Inconsistent Redis or broker configuration detected"
  exit $EXIT_KEYS
fi
if ! eval "$BUS_EXPORTS"; then
  log_warn "Failed to apply canonical Redis or broker configuration"
  exit $EXIT_KEYS
fi
log_info "RUNTIME_MANIFEST channel=$BROKER_CHANNEL redis=$REDIS_URL mint_stream=$MINT_STREAM_REDIS_URL mempool=$MEMPOOL_STREAM_REDIS_URL amm_watch=$AMM_WATCH_REDIS_URL bus=$EVENT_BUS_URL"

SOCKET_STATE_RAW=$(wait_for_socket_release)
read -r SOCKET_STATE SOCKET_HOST SOCKET_PORT SOCKET_REMOTE <<<"$SOCKET_STATE_RAW"
SOCKET_HOST=${SOCKET_HOST:-127.0.0.1}
SOCKET_PORT=${SOCKET_PORT:-8779}
SOCKET_REMOTE=${SOCKET_REMOTE:-}
if [[ $SOCKET_STATE == "busy" ]]; then
  log_warn "Event bus port ${SOCKET_HOST}:${SOCKET_PORT} is still in use after the grace window; aborting launch"
  log_warn "Hint: terminate the lingering process (e.g. pkill -f 'event_bus') and run bash scripts/clean_session.sh to clear stale locks before retrying"
  exit $EXIT_SOCKET
else
  if [[ $SOCKET_REMOTE == "remote" ]]; then
    log_info "Event bus port ${SOCKET_HOST}:${SOCKET_PORT} ready (remote endpoint; skipping local socket release verification)"
  else
    log_info "Event bus port ${SOCKET_HOST}:${SOCKET_PORT} ready"
  fi
fi

UI_SOCKET_STATE_RAW=$(wait_for_ui_socket_release)
read -r UI_SOCKET_STATE UI_SOCKET_HOST UI_SOCKET_PORT UI_SOCKET_REASON <<<"$UI_SOCKET_STATE_RAW"
UI_SOCKET_HOST=${UI_SOCKET_HOST:-${UI_HOST:-127.0.0.1}}
UI_SOCKET_PORT=${UI_SOCKET_PORT:-${UI_PORT:-5001}}
UI_SOCKET_REASON=${UI_SOCKET_REASON:-}
if [[ $UI_SOCKET_STATE == "busy" ]]; then
  log_warn "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} is still in use after the grace window; aborting launch"
  log_warn "Hint: terminate the process bound to ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} or adjust UI_HOST/UI_PORT before retrying"
  exit $EXIT_SOCKET
else
  case $UI_SOCKET_REASON in
    remote)
      log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} ready (remote endpoint; skipping local socket release verification)"
      ;;
    disabled)
      log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} check skipped (UI port disabled)"
      ;;
    *)
      log_info "UI port ${UI_SOCKET_HOST}:${UI_SOCKET_PORT} ready"
      ;;
  esac
fi

log_info "Ensuring Redis availability"
ensure_redis
log_info "Redis helper completed"

log_info "Running Redis health check"
if ! redis_health; then
  log_warn "Redis health check failed"
  exit $EXIT_HEALTH
fi
log_info "Redis health check passed"

declare -a CHILD_PIDS=()
register_child() {
  CHILD_PIDS+=("$1")
}

cleanup() {
  release_runtime_lock
  if [[ -z ${CHILD_PIDS+x} ]]; then
    return
  fi
  for pid in "${CHILD_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
}

acquire_runtime_lock
trap cleanup EXIT
trap 'cleanup; exit $EXIT_HEALTH' ERR
trap 'cleanup; exit 0' INT TERM

ORIG_SOLHUNTER_MODE=${SOLHUNTER_MODE-}
ORIG_MODE=${MODE-}
export SOLHUNTER_MODE="${SOLHUNTER_MODE:-paper}"
export MODE="${MODE:-paper}"
run_schema_smoke_tests
if [[ -n ${ORIG_SOLHUNTER_MODE:-} ]]; then
  export SOLHUNTER_MODE="$ORIG_SOLHUNTER_MODE"
else
  unset SOLHUNTER_MODE
fi
if [[ -n ${ORIG_MODE:-} ]]; then
  export MODE="$ORIG_MODE"
else
  unset MODE
fi

start_log_stream() {
  local log=$1
  local label=$2
  # Stream existing and new log entries without truncating the file so we do
  # not drop output emitted before the streamer attaches.
  local __last_line=""
  {
    tail -n +1 -F "$log" 2>/dev/null |
      while IFS= read -r line; do
        if [[ ${line} != "${__last_line:-}" ]]; then
          printf '[%s] [%s] %s\n' "$(timestamp)" "$label" "$line"
        fi
        __last_line=$line
      done
  } &
  local tail_pid=$!
  register_child "$tail_pid"
}

start_controller() {
  local mode=$1
  local log=$2
  local notify=$3
  # Truncate the log before launching the controller to drop any stale output.
  : > "$log"
  local args=("$ROOT_DIR/scripts/live_runtime_controller.py" "--mode" "$mode" "--micro" "$MICRO_FLAG")
  if [[ -n $CONFIG_PATH ]]; then
    args+=("--config" "$CONFIG_PATH")
  fi
  if [[ $mode == "live" && $CANARY_MODE -eq 1 ]]; then
    args+=("--canary-budget" "$CANARY_BUDGET" "--canary-risk" "$CANARY_RISK")
  fi
  if [[ -n $notify ]]; then
    args+=("--notify" "$notify")
  fi
  "$PYTHON_BIN" "${args[@]}" >"$log" 2>&1 &
  local pid=$!
  register_child "$pid"
  printf '%s\n' "$pid"
}

print_log_excerpt() {
  local log=$1
  local reason=${2:-}
  local header="---- Runtime log excerpt ($log) ----"
  if [[ -n $reason ]]; then
    echo "$reason" >&2
  fi
  if [[ -f $log ]]; then
    echo "$header" >&2
    tail -n 200 "$log" >&2 || true
    echo "---- End runtime log ----" >&2
  else
    echo "Log file $log not found" >&2
  fi
}

print_ui_location() {
  local runtime_log=$1
  local art_dir=$2
  local ui_line=""
  if [[ -f $runtime_log ]]; then
    ui_line="$(grep -m1 -E 'UI_READY url=' "$runtime_log" || true)"
  fi
  if [[ -z $ui_line && -n $art_dir && -f "$art_dir/ui_url.txt" ]]; then
    ui_line="UI_READY url=$(cat "$art_dir/ui_url.txt")"
  fi
  if [[ -n $ui_line ]]; then
    local ui_url
    ui_url="$(echo "$ui_line" | sed -E 's/.*url=([^ ]+).*/\1/')"
    if [[ -n $ui_url ]]; then
      echo ""
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo "  UI available at: $ui_url"
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    else
      echo "UI readiness line detected but URL parsing failed: $ui_line"
    fi
  else
    echo "UI URL not detected yet; check $runtime_log for binding lines."
  fi
}

READY_TIMEOUT="${READY_TIMEOUT:-120}"
wait_for_ready() {
  local log=$1
  local notify=$2
  local pid=${3:-}
  local waited=0
  local ui_seen=0
  local bus_seen=0
  local ui_ws_seen=0
  local golden_seen=0
  local golden_disabled_seen=0
  local runtime_seen=0
  while [[ $waited -lt $READY_TIMEOUT ]]; do
    if [[ -n $notify && -f $notify ]]; then
      runtime_seen=1
    fi
    if [[ $ui_seen -eq 0 ]] && grep -q "UI_READY" "$log" 2>/dev/null; then
      ui_seen=1
    fi
    if [[ $ui_ws_seen -eq 0 ]] && grep -q "UI_WS_READY" "$log" 2>/dev/null; then
      ui_ws_seen=1
    fi
    if [[ $bus_seen -eq 0 ]] && grep -q "Event bus: connected" "$log" 2>/dev/null; then
      bus_seen=1
    fi
    if [[ $golden_seen -eq 0 ]]; then
      if grep -q "GOLDEN_READY" "$log" 2>/dev/null; then
        golden_seen=1
      elif [[ $golden_disabled_seen -eq 0 ]] && grep -qE "golden:start[^\\n]*disabled" "$log" 2>/dev/null; then
        golden_disabled_seen=1
        golden_seen=1
      fi
    fi
    if [[ $runtime_seen -eq 0 ]] && grep -q "RUNTIME_READY" "$log" 2>/dev/null; then
      runtime_seen=1
    fi
    if [[ $ui_seen -eq 1 && $ui_ws_seen -eq 1 && $bus_seen -eq 1 && $golden_seen -eq 1 && $runtime_seen -eq 1 ]]; then
      return 0
    fi
    if [[ -n $pid ]] && ! kill -0 "$pid" >/dev/null 2>&1; then
      print_log_excerpt "$log" "Runtime process $pid exited early"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
  done
  local missing=""
  if [[ $ui_seen -eq 0 ]]; then
    missing+=" UI_READY"
  fi
  if [[ $ui_ws_seen -eq 0 ]]; then
    missing+=" UI_WS_READY"
  fi
  if [[ $bus_seen -eq 0 ]]; then
    missing+=" Event bus"
  fi
  if [[ $golden_seen -eq 0 ]]; then
    missing+=" GOLDEN_READY"
  fi
  if [[ $runtime_seen -eq 0 ]]; then
    missing+=" RUNTIME_READY"
  fi
  print_log_excerpt "$log" "Timed out waiting for runtime readiness (missing:${missing:- none})"
  return 1
}

PAPER_LOG="$LOG_DIR/paper_runtime.log"
PAPER_NOTIFY="$ARTIFACT_DIR/paper_ready"
rm -f "$PAPER_NOTIFY"
log_info "Launching runtime controller (mode=paper, log=$PAPER_LOG)"
PAPER_PID=$(start_controller "paper" "$PAPER_LOG" "$PAPER_NOTIFY")
start_log_stream "$PAPER_LOG" "paper"
log_info "Waiting for paper runtime readiness"
if ! wait_for_ready "$PAPER_LOG" "$PAPER_NOTIFY" "$PAPER_PID"; then
  log_warn "Paper runtime failed to become ready"
  exit $EXIT_HEALTH
fi

log_info "Paper runtime ready (PID=$PAPER_PID)"
print_ui_location "$PAPER_LOG" "$ART_DIR"
if ! check_ui_health "$PAPER_LOG"; then
  log_warn "Paper runtime UI health check failed"
  exit $EXIT_HEALTH
fi

run_preflight() {
  local mode=$1
  local -a micro_settings=(1 0)
  local expected_runs=${#micro_settings[@]}

  if [[ $PREFLIGHT_RUNS -ne $expected_runs ]]; then
    log_warn "PREFLIGHT_RUNS=$PREFLIGHT_RUNS but expected $expected_runs passes for micro on/off"
    return 1
  fi

  local idx=1
  for micro in "${micro_settings[@]}"; do
    log_info "Running preflight suite (mode=$mode micro=$micro pass $idx/$PREFLIGHT_RUNS)"
    if ! MODE="$mode" MICRO_MODE="$micro" bash "$ROOT_DIR/scripts/preflight/run_all.sh"; then
      return 1
    fi
    ((idx++))
  done
}

log_info "Running preflight suite"
if ! run_preflight "paper"; then
  log_warn "Preflight suite failed"
  exit $EXIT_PREFLIGHT
fi
log_info "Preflight suite completed successfully"

kill "$PAPER_PID" >/dev/null 2>&1 || true
wait "$PAPER_PID" 2>/dev/null || true
log_info "Paper runtime stopped after preflight"

export CONNECTIVITY_SKIP_UI_PROBES=1
log_info "Paper runtime offline; skipping UI connectivity probes during soak"
log_info "Starting connectivity soak for ${SOAK_DURATION}s"
connectivity_soak() {
  "$PYTHON_BIN" - <<'PY'
import json
import os
from pathlib import Path
from solhunter_zero.production import ConnectivityChecker

duration = float(os.environ.get("SOAK_DURATION", "0"))
output_path = Path(os.environ.get("SOAK_REPORT", "artifacts/prelaunch/connectivity_report.json"))
checker = ConnectivityChecker()
async def _run():
    summary = await checker.run_soak(duration=duration, output_path=output_path)
    return {
        "duration": summary.duration,
        "reconnect_count": summary.reconnect_count,
        "metrics": summary.metrics,
        "report": str(output_path),
    }

import asyncio
result = asyncio.run(_run())
print(json.dumps(result))
PY
}

validate_connectivity_soak() {
  local soak_json=$1
  local report_path=${2:-}
  local analysis=""
  export SOAK_RESULT_JSON="$soak_json"
  if ! analysis=$("$PYTHON_BIN" - <<'PY'
import json
import os
import sys

payload = os.environ.get("SOAK_RESULT_JSON")
if payload is None:
    sys.exit(0)

try:
    result = json.loads(payload)
except Exception as exc:
    print(f"invalid JSON: {exc}", end="")
    sys.exit(1)

issues: list[str] = []

reconnect_count = result.get("reconnect_count")
if isinstance(reconnect_count, (int, float)) and reconnect_count:
    issues.append(f"reconnect_count={int(reconnect_count)}")

metrics = result.get("metrics")
if isinstance(metrics, dict):
    for name, stats in metrics.items():
        if not isinstance(stats, dict):
            continue
        errors = stats.get("errors")
        if isinstance(errors, dict):
            total = 0
            parts: list[str] = []
            for key, value in sorted(errors.items()):
                if isinstance(value, (int, float)) and value:
                    total += int(value)
                    parts.append(f"{key}={int(value)}")
            if total:
                if parts:
                    issues.append(f"{name} errors: {', '.join(parts)}")
                else:
                    issues.append(f"{name} errors: {total}")
        elif errors:
            issues.append(f"{name} errors: {errors}")

if issues:
    print("; ".join(issues), end="")
    sys.exit(1)
PY
  ); then
    local status=$?
    if [[ $status -eq 0 ]]; then
      status=1
    fi
    if [[ -z ${analysis:-} ]]; then
      analysis="connectivity soak validation failed"
    fi
    if [[ -n ${report_path:-} ]]; then
      analysis+=" (report=${report_path})"
    fi
    log_warn "Connectivity soak reported failures: $analysis"
    unset SOAK_RESULT_JSON
    return "$status"
  fi
  unset SOAK_RESULT_JSON
  return 0
}

export SOAK_DURATION="$SOAK_DURATION"
export SOAK_REPORT="$ARTIFACT_DIR/connectivity_report.json"
SOAK_RESULT=""
if ! SOAK_RESULT=$(connectivity_soak); then
  log_warn "Connectivity soak failed"
  exit $EXIT_CONNECTIVITY
fi

if ! validate_connectivity_soak "$SOAK_RESULT" "$SOAK_REPORT"; then
  exit $EXIT_CONNECTIVITY
fi

log_info "Connectivity soak complete: $SOAK_RESULT"
unset CONNECTIVITY_SKIP_UI_PROBES
log_info "UI connectivity probes re-enabled for live launch"

export MODE=live
# keep env aligned with the flag we passed to the controller
export MICRO_MODE="$MICRO_FLAG"
if [[ $CANARY_MODE -eq 1 ]]; then
  export CANARY_MODE=1
  export CANARY_BUDGET_USD=$CANARY_BUDGET
  export CANARY_RISK_CAP=$CANARY_RISK
fi

LIVE_LOG="$LOG_DIR/live_runtime.log"
LIVE_NOTIFY="$ARTIFACT_DIR/live_ready"
rm -f "$LIVE_NOTIFY"
log_info "Launching runtime controller (mode=live, log=$LIVE_LOG)"
LIVE_PID=$(start_controller "live" "$LIVE_LOG" "$LIVE_NOTIFY")
start_log_stream "$LIVE_LOG" "live"
log_info "Waiting for live runtime readiness"
if ! wait_for_ready "$LIVE_LOG" "$LIVE_NOTIFY" "$LIVE_PID"; then
  log_warn "Live runtime failed to become ready"
  exit $EXIT_HEALTH
fi

log_info "Live runtime ready (PID=$LIVE_PID)"
print_ui_location "$LIVE_LOG" "$ART_DIR"
if ! check_ui_health "$LIVE_LOG"; then
  log_warn "Live runtime UI health check failed"
  exit $EXIT_HEALTH
fi
micro_label=$([[ "$MICRO_FLAG" == "1" ]] && echo "on" || echo "off")
canary_label=$([[ $CANARY_MODE -eq 1 ]] && echo " | Canary limits applied" || echo "")
GO_NO_GO="GO/NO-GO: Keys OK | Services OK | Preflight PASSED (2/2) | Soak PASSED | MODE=live | MICRO=${micro_label}${canary_label}"
log_info "$GO_NO_GO"

wait "$LIVE_PID"
status=$?
if [[ $status -ne 0 ]]; then
  print_log_excerpt "$LIVE_LOG" "Live runtime exited with status $status"
  log_warn "Live runtime exited with status $status"
  exit $EXIT_HEALTH
fi

log_info "Live runtime exited cleanly"

exit 0
