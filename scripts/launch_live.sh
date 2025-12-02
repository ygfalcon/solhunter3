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

ensure_repo_pythonpath() {
  if [[ -n ${PYTHONPATH:-} ]]; then
    case ":$PYTHONPATH:" in
      *":$ROOT_DIR:"*)
        ;;
      *)
        export PYTHONPATH="$ROOT_DIR:$PYTHONPATH"
        ;;
    esac
  else
    export PYTHONPATH="$ROOT_DIR"
  fi
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

append_offline_remediation_hint() {
  local deps_log=$1
  local artifact_root="${RUNTIME_ARTIFACT_ROOT:-$DEFAULT_ARTIFACT_ROOT}"
  {
    echo
    echo "Offline remediation: add requirements*.lock files or wheel artifacts under $artifact_root for fallback installs."
  } >>"$deps_log"
}

artifact_fallback_install() {
  local deps_log=$1
  local artifact_root="${RUNTIME_ARTIFACT_ROOT:-$DEFAULT_ARTIFACT_ROOT}"
  local -a lock_files=()
  while IFS= read -r -d '' file; do
    lock_files+=("$file")
  done < <(find "$artifact_root" -type f -name 'requirements*.lock' -print0 2>/dev/null)

  local -a wheel_dirs=()
  while IFS= read -r -d '' wheel; do
    wheel_dirs+=("$(dirname "$wheel")")
  done < <(find "$artifact_root" -type f -name '*.whl' -print0 2>/dev/null)

  if (( ${#wheel_dirs[@]} )); then
    mapfile -t wheel_dirs < <(printf '%s\n' "${wheel_dirs[@]}" | sort -u)
  fi

  if (( ${#lock_files[@]} == 0 && ${#wheel_dirs[@]} == 0 )); then
    return 1
  fi

  local -a pip_args=(install --no-index)
  for dir in "${wheel_dirs[@]}"; do
    pip_args+=(--find-links "$dir")
  done

  if (( ${#lock_files[@]} )); then
    for lock_file in "${lock_files[@]}"; do
      pip_args+=(-r "$lock_file")
    done
  else
    pip_args+=(-r "$ROOT_DIR/requirements.txt" -r "$ROOT_DIR/requirements-tests.txt" "jsonschema[format-nongpl]==4.23.0")
  fi

  log_info "Attempting offline installation from artifacts at $artifact_root"
  set +e
  {
    "$PIP_BIN" "${pip_args[@]}"
  } 2>&1 | tee -a "$deps_log" >/dev/null
  local install_status=${PIPESTATUS[0]:-1}
  local tee_status=${PIPESTATUS[1]:-0}
  set -e
  if [[ $install_status -ne 0 || $tee_status -ne 0 ]]; then
    return 1
  fi

  return 0
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

RUNTIME_CACHE_DIR="$RUNTIME_ARTIFACT_ROOT/.cache"
RUNTIME_FS_LOCK="$RUNTIME_CACHE_DIR/runtime.lock"
RUNTIME_LOCK_ATTEMPTED=0
RUNTIME_LOCK_ACQUIRED=0
RUNTIME_FS_LOCK_WRITTEN=0
RUNTIME_FS_LOCK_PAYLOAD=""
RUNTIME_LOCK_REFRESH_PID=0
LIVE_MODE_ENV_APPLIED=0
LIVE_MODE_PREVIOUS=""
LIVE_MODE_WAS_SET=0
LIVE_SOLHUNTER_MODE_PREVIOUS=""
LIVE_SOLHUNTER_MODE_WAS_SET=0
CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS=""
CONNECTIVITY_SKIP_UI_PROBES_WAS_SET=0

export RUNTIME_LOCK_TTL_SECONDS="${RUNTIME_LOCK_TTL_SECONDS:-60}"
export RUNTIME_LOCK_REFRESH_INTERVAL="${RUNTIME_LOCK_REFRESH_INTERVAL:-20}"

# Ensure the repository root is always importable when invoking helper scripts.
# "launch_live.sh" may be executed before the package is installed (e.g. from a
# fresh clone), so python invocations need the project root on PYTHONPATH so
# that modules like ``solhunter_zero`` can be imported successfully.
ensure_repo_pythonpath

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
    set +e
    {
      "$PIP_BIN" install --no-index "${find_links[@]}" -r "$ROOT_DIR/requirements.txt" -r "$ROOT_DIR/requirements-tests.txt" \
        "jsonschema[format-nongpl]==4.23.0"
    } 2>&1 | tee "$DEPS_LOG" >/dev/null
    local install_status=${PIPESTATUS[0]:-1}
    local tee_status=${PIPESTATUS[1]:-0}
    set -e
    if [[ $install_status -ne 0 || $tee_status -ne 0 ]]; then
      log_warn "Failed to install Python dependencies from cache; checking artifacts for offline fallback"
      append_offline_remediation_hint "$DEPS_LOG"
      if ! artifact_fallback_install "$DEPS_LOG"; then
        log_warn "Failed to install Python dependencies after artifact fallback (see $DEPS_LOG)"
        exit $EXIT_DEPS
      fi
    fi
    if ! "$PIP_BIN" check >/dev/null 2>&1; then
      log_warn "Offline installation succeeded but dependency check still reports issues (see $DEPS_LOG for cached wheel details)"
      exit $EXIT_DEPS
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
explicit_event_bus = bool(os.environ.get("EVENT_BUS_URL"))
if not explicit_event_bus:
    os.environ.setdefault("EVENT_BUS_URL", DEFAULT_BUS)

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


def _load_json_urls(env: str) -> list[str]:
    raw = os.environ.get(env)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        errors.append(f"{env} is not valid JSON: {raw}")
        return []
    urls: list[str] = []
    if isinstance(data, str):
        candidate = data.strip()
        if candidate:
            urls.append(candidate)
        else:
            errors.append(f"{env} contains an empty string")
        return urls
    if not isinstance(data, (list, tuple)):
        errors.append(f"{env} must be a JSON array of URLs: {raw}")
        return []
    for entry in data:
        if isinstance(entry, str):
            candidate = entry.strip()
            if candidate:
                urls.append(candidate)
            else:
                errors.append(f"{env} includes an empty string entry")
        else:
            errors.append(f"{env} includes non-string entry: {entry!r}")
    return urls


def _normalize_multi_key(
    key: str,
    raw: str | None,
    *,
    force_db: int | None = None,
    extras: list[str] | None = None,
) -> list[str]:
    candidates: list[str] = []
    if raw:
        os.environ[key] = raw
        candidates.extend(
            segment.strip() for segment in raw.split(",") if segment.strip()
        )
    elif extras:
        # ensure key absent when only extras supply candidates
        os.environ.pop(key, None)
    if extras:
        candidates.extend(url for url in extras if url)
    canonical_parts: list[str] = []
    redacted_parts: list[str] = []
    for part in candidates:
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
    return canonical_parts

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
json_urls = _load_json_urls("BROKER_URLS_JSON")
canonical_brokers = _normalize_multi_key(
    "BROKER_URLS",
    os.environ.get("BROKER_URLS"),
    force_db=1,
    extras=json_urls,
)

if canonical_brokers and not explicit_event_bus:
    candidate_bus = canonical_brokers[0]
    parsed_bus = urlparse(candidate_bus)
    if parsed_bus.scheme in {"redis", "rediss"}:
        os.environ["EVENT_BUS_URL"] = candidate_bus

bus_url = os.environ.get("EVENT_BUS_URL", DEFAULT_BUS)

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
urls: list[str] = []
seen: set[str] = set()


def _append_urls(raw: str | None) -> None:
    if not raw:
        return
    for part in raw.split(','):
        trimmed = part.strip()
        if not trimmed or trimmed in seen:
            continue
        seen.add(trimmed)
        urls.append(trimmed)


for key, value in os.environ.items():
    if key.endswith("_REDIS_URL"):
        _append_urls(value)

for key in ("REDIS_URL", "BROKER_URL", "BROKER_URLS"):
    _append_urls(os.getenv(key))
ensure_local_redis_if_needed(urls)
PY
}

redis_health() {
  local -a redis_urls=()
  mapfile -t redis_urls < <(
    "$PYTHON_BIN" - <<'PY'
import os
from collections import OrderedDict

keys = [
    "REDIS_URL",
    "MINT_STREAM_REDIS_URL",
    "MEMPOOL_STREAM_REDIS_URL",
    "AMM_WATCH_REDIS_URL",
    "BROKER_URL",
    "BROKER_URLS",
]

urls = OrderedDict()
for key in keys:
    raw = os.environ.get(key)
    if not raw:
        continue
    for part in raw.split(","):
        candidate = (part or "").strip()
        if candidate:
            urls.setdefault(candidate, None)

print("\n".join(urls.keys()), end="")
PY
  )

  if (( ${#redis_urls[@]} == 0 )); then
    return 0
  fi

  local url
  local redis_cli_usage_fallback=0
  if command -v redis-cli >/dev/null 2>&1; then
    for url in "${redis_urls[@]}"; do
      local redis_cli_output=""
      if ! redis_cli_output=$(redis-cli -u "$url" PING 2>&1); then
        local normalized_output=${redis_cli_output,,}
        if [[ $normalized_output == usage:* || $normalized_output == *$'\n'usage:* || $normalized_output == *unknown\ option* ]]; then
          redis_cli_usage_fallback=1
          break
        fi
        printf '%s\n' "$redis_cli_output" >&2
        return 1
      fi
    done
    if (( redis_cli_usage_fallback == 0 )); then
      return 0
    fi
  fi

  if ! "$PYTHON_BIN" - "${redis_urls[@]}" <<'PY'
import asyncio
import sys

from solhunter_zero.production import ConnectivityChecker

urls = sys.argv[1:]


async def _check_all(targets):
    checker = ConnectivityChecker()
    probe = getattr(checker, "_probe_redis", None)
    if probe is None:
        raise SystemExit(1)
    for idx, target in enumerate(targets, start=1):
        result = await probe(f"redis-{idx}", target)
        if not getattr(result, "ok", False):
            raise SystemExit(1)


asyncio.run(_check_all(urls))
PY
    then
      return 1
    fi
}

rl_health_check() {
  "$PYTHON_BIN" - <<'PY'
import json
import sys

from solhunter_zero.health_runtime import (
    check_rl_daemon_health,
    resolve_rl_health_url,
)


def main() -> int:
    try:
        url = resolve_rl_health_url(require_health_file=True)
    except Exception as exc:  # pragma: no cover - configuration guard
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1

    ok, msg = check_rl_daemon_health(url, require_health_file=True)
    print(json.dumps({"ok": bool(ok), "url": url, "message": msg}))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
PY
}

run_connectivity_probes() {
  "$PYTHON_BIN" - <<'PY'
import asyncio
import ipaddress
import os
import sys
from urllib.parse import urlparse

from solhunter_zero.production import ConnectivityChecker


REQUIRED_TARGETS = [
    ("redis", "Redis"),
    ("solana-rpc", "Solana RPC"),
    ("solana-ws", "Solana WebSocket"),
    ("helius-rest", "Helius REST"),
    ("helius-das", "Helius DAS"),
    ("jito-rpc", "Jito RPC"),
    ("jito-ws", "Jito WebSocket"),
    ("mempool-stream-ws", "Mempool stream WebSocket"),
    ("mempool-stream-redis", "Mempool stream Redis"),
    ("event-bus", "Event bus"),
]

UI_TARGETS = [
    ("ui-ws", "UI WebSocket"),
    ("ui-http", "UI HTTP"),
]


def _format_result(label: str, result) -> str:
    if result is None:
        return f"{label}: missing target"
    status = "OK" if result.ok else f"FAIL ({result.error or result.status or result.status_code})"
    latency = f"{result.latency_ms:.1f} ms" if result.latency_ms is not None else "n/a"
    return f"{label}: {status} → {result.target} ({latency})"


def _runtime_bus_target(raw_url: str | None) -> tuple[str, int, str]:
    default = urlparse("ws://127.0.0.1:8779")
    parsed = urlparse(raw_url or default.geturl())
    host = parsed.hostname or default.hostname or "127.0.0.1"
    scheme = (parsed.scheme or default.scheme or "ws").lower()
    if parsed.port is not None:
        port = parsed.port
    elif scheme == "wss":
        port = 443
    elif scheme == "ws":
        port = 80
    else:
        port = default.port or 8779
    if raw_url:
        display = raw_url
    else:
        display = f"ws://{host}:{port}"
    return host, port, display


def _is_local_host(hostname: str) -> bool:
    normalized = (hostname or "").strip().lower()
    if not normalized or normalized == "localhost":
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return address.is_loopback


def _env_flag(name: str) -> bool:
    raw = os.environ.get(name, "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


async def _run() -> None:
    checker = ConnectivityChecker()
    results = await checker.check_all()
    lookup = {result.name: result for result in results}

    raw_bus_url = os.environ.get("EVENT_BUS_URL")
    bus_result = None
    bus_skip_message: str | None = None
    skip_ui = _env_flag("CONNECTIVITY_SKIP_UI_PROBES")

    if raw_bus_url:
        bus_host, bus_port, bus_display = _runtime_bus_target(raw_bus_url)
        if _is_local_host(bus_host):
            target = bus_display or f"ws://{bus_host}:{bus_port}"
            bus_skip_message = (
                "Event bus: SKIPPED → runtime-managed local endpoint "
                f"{target} (post-launch readiness checks will verify availability)"
            )
        else:
            try:
                bus_result = await checker._probe_ws("event-bus", raw_bus_url)
            except AttributeError as exc:  # pragma: no cover - defensive
                print(f"ConnectivityChecker missing WebSocket probe support: {exc}", file=sys.stderr)
                raise SystemExit(1)
            lookup["event-bus"] = bus_result
    else:
        lookup.setdefault("event-bus", None)

    failures: list[str] = []
    for key, label in REQUIRED_TARGETS:
        if key == "event-bus" and bus_skip_message:
            print(bus_skip_message)
            continue

        result = lookup.get(key)
        print(_format_result(label, result))
        if result is None:
            failures.append(f"{label} connectivity probe skipped (no target configured)")
            continue
        if not result.ok:
            reason = result.error or result.status or "unavailable"
            failures.append(f"{label} connectivity check failed: {reason} ({result.target})")

    for key, label in UI_TARGETS:
        result = lookup.get(key)
        if result is None:
            if skip_ui:
                print(
                    f"{label}: SKIPPED → UI connectivity probes disabled"
                    " (CONNECTIVITY_SKIP_UI_PROBES=1)"
                )
                continue
            print(_format_result(label, result))
            failures.append(f"{label} connectivity probe skipped (no target configured)")
            continue
        print(_format_result(label, result))
        if not result.ok:
            reason = result.error or result.status or "unavailable"
            failures.append(f"{label} connectivity check failed: {reason} ({result.target})")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        raise SystemExit(1)


asyncio.run(_run())
PY
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
scheme = (parsed.scheme or "ws").lower()
if parsed.port is not None:
    port = parsed.port
elif scheme == "wss":
    port = 443
elif scheme == "ws":
    port = 80
else:
    port = 8779
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
  stop_runtime_lock_refresher
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
try:
    ttl_seconds = int(os.environ.get("RUNTIME_LOCK_TTL_SECONDS", "60"))
except ValueError:
    ttl_seconds = 60
if ttl_seconds <= 0:
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
  if [[ -n ${RUNTIME_FS_LOCK:-} ]]; then
    local fs_lock_dir=""
    fs_lock_dir=$(dirname -- "$RUNTIME_FS_LOCK")
    if mkdir -p "$fs_lock_dir"; then
      local fs_lock_host="unknown"
      if ! fs_lock_host=$(hostname 2>/dev/null); then
        fs_lock_host="unknown"
      fi
      local fs_lock_pid="${BASHPID:-$$}"
      local fs_lock_payload="pid=${fs_lock_pid} host=${fs_lock_host} token=${RUNTIME_LOCK_TOKEN}"
      if printf '%s\n' "$fs_lock_payload" >"$RUNTIME_FS_LOCK"; then
        RUNTIME_FS_LOCK_WRITTEN=1
        RUNTIME_FS_LOCK_PAYLOAD="$fs_lock_payload"
        log_info "Recorded runtime filesystem lock at $RUNTIME_FS_LOCK"
      else
        log_warn "Failed to write runtime filesystem lock at $RUNTIME_FS_LOCK"
      fi
    else
      log_warn "Failed to prepare runtime filesystem lock directory $fs_lock_dir"
    fi
  fi
  start_runtime_lock_refresher
  if [[ -z ${RUNTIME_LOCK_REFRESH_PID:-} || $RUNTIME_LOCK_REFRESH_PID -le 0 ]]; then
    log_warn "Runtime lock refresher did not start; lock TTL may expire early"
    exit $EXIT_HEALTH
  elif ! kill -0 "$RUNTIME_LOCK_REFRESH_PID" >/dev/null 2>&1; then
    log_warn "Runtime lock refresher exited prematurely (pid=$RUNTIME_LOCK_REFRESH_PID)"
    exit $EXIT_HEALTH
  fi
}

release_runtime_lock() {
  stop_runtime_lock_refresher
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

start_runtime_lock_refresher() {
  if [[ -z ${RUNTIME_LOCK_KEY:-} || -z ${RUNTIME_LOCK_TOKEN:-} ]]; then
    return
  fi
  if [[ -n ${RUNTIME_LOCK_REFRESH_PID:-} && $RUNTIME_LOCK_REFRESH_PID -gt 0 ]]; then
    if kill -0 "$RUNTIME_LOCK_REFRESH_PID" >/dev/null 2>&1; then
      return
    fi
  fi
  log_info "Starting runtime lock refresher (interval ${RUNTIME_LOCK_REFRESH_INTERVAL}s, ttl ${RUNTIME_LOCK_TTL_SECONDS}s)"
  (
    export RUNTIME_LOCK_KEY
    export RUNTIME_LOCK_TOKEN
    export RUNTIME_LOCK_REFRESH_INTERVAL
    export RUNTIME_LOCK_TTL_SECONDS
    "$PYTHON_BIN" "$ROOT_DIR/scripts/runtime_lock_refresher.py"
  ) &
  RUNTIME_LOCK_REFRESH_PID=$!
}

stop_runtime_lock_refresher() {
  if [[ -z ${RUNTIME_LOCK_REFRESH_PID:-} ]]; then
    return
  fi
  if (( RUNTIME_LOCK_REFRESH_PID <= 0 )); then
    RUNTIME_LOCK_REFRESH_PID=0
    return
  fi
  if kill -0 "$RUNTIME_LOCK_REFRESH_PID" >/dev/null 2>&1; then
    kill "$RUNTIME_LOCK_REFRESH_PID" >/dev/null 2>&1 || true
    wait "$RUNTIME_LOCK_REFRESH_PID" 2>/dev/null || true
    log_info "Stopped runtime lock refresher (pid=$RUNTIME_LOCK_REFRESH_PID)"
  fi
  RUNTIME_LOCK_REFRESH_PID=0
}

runtime_lock_ttl_check() {
  local context=${1:-unknown}
  if [[ -z ${RUNTIME_LOCK_KEY:-} || -z ${RUNTIME_LOCK_TOKEN:-} ]]; then
    return 0
  fi
  local output status=0
  output=$(RUNTIME_LOCK_KEY="$RUNTIME_LOCK_KEY" \
    RUNTIME_LOCK_TOKEN="$RUNTIME_LOCK_TOKEN" \
    "$PYTHON_BIN" - <<'PY') || status=$?
import json
import os
import sys

try:
    import redis  # type: ignore
except Exception as exc:  # pragma: no cover - dependency issue
    print(f"redis import failed: {exc}")
    sys.exit(1)

key = os.environ.get("RUNTIME_LOCK_KEY")
token = os.environ.get("RUNTIME_LOCK_TOKEN")
refresh_interval = float(os.environ.get("RUNTIME_LOCK_REFRESH_INTERVAL", "20"))
redis_url = os.environ.get("REDIS_URL") or "redis://localhost:6379/1"

client = redis.Redis.from_url(redis_url, socket_timeout=1.0)
payload_raw = client.get(key)
if not payload_raw:
    print("runtime lock missing")
    sys.exit(1)

try:
    payload = json.loads(payload_raw.decode("utf-8"))
except Exception:
    print("runtime lock payload decode failed")
    sys.exit(1)

if payload.get("token") != token:
    print("runtime lock token mismatch")
    sys.exit(1)

ttl = client.ttl(key)
if ttl is None or ttl < 0:
    print("runtime lock ttl unavailable")
    sys.exit(1)

if ttl <= refresh_interval:
    print(f"{ttl}")
    sys.exit(1)

print(f"{ttl}")
PY
  if (( status != 0 )); then
    log_warn "Runtime lock TTL check failed during $context: $output"
  else
    log_info "Runtime lock TTL during $context: ${output}s (refresh interval ${RUNTIME_LOCK_REFRESH_INTERVAL}s)"
  fi
  return $status
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
  local -a attempted_sources=("runtime log scrape")
  local artifact_candidate=""
  if [[ -z $url && -n ${ARTIFACT_DIR:-} ]]; then
    local artifact_ui_path="$ARTIFACT_DIR/ui_url.txt"
    attempted_sources+=("$artifact_ui_path")
    artifact_candidate=$(ARTIFACT_UI_FILE="$artifact_ui_path" "$PYTHON_BIN" - <<'PY'
import os
import sys

path = os.environ.get("ARTIFACT_UI_FILE")
if not path or not os.path.exists(path):
    sys.exit(0)

with open(path, "r", encoding="utf-8", errors="ignore") as handle:
    value = handle.read().strip()

if value:
    print(value)
PY
    )
    if [[ -n $artifact_candidate ]]; then
      url=$artifact_candidate
      log_info "UI readiness URL resolved via artifact file $artifact_ui_path"
    fi
  fi
  if [[ -z $url && -n ${UI_HTTP_URL:-} ]]; then
    attempted_sources+=("UI_HTTP_URL environment variable")
    local env_candidate=""
    env_candidate=$(UI_HEALTH_ENV_URL="$UI_HTTP_URL" "$PYTHON_BIN" - <<'PY'
import os
import sys

value = os.environ.get("UI_HEALTH_ENV_URL", "")
value = value.strip()

if value:
    print(value)
PY
    )
    if [[ -n $env_candidate ]]; then
      url=$env_candidate
      log_info "UI readiness URL resolved via UI_HTTP_URL environment variable"
    fi
  fi
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
    local IFS=", "
    log_warn "UI readiness URL not yet available for health check (checked ${attempted_sources[*]})"
    return 1
  fi
  local target="${url%/}/ui/meta"
  local total_timeout_default=30
  local total_timeout_raw="${READY_TIMEOUT:-}"
  local total_timeout=$total_timeout_default
  if [[ -z $total_timeout_raw ]]; then
    total_timeout=$total_timeout_default
  elif [[ $total_timeout_raw =~ ^[0-9]+$ ]]; then
    total_timeout=$total_timeout_raw
  else
    log_warn "READY_TIMEOUT='$total_timeout_raw' is not a valid integer; aborting UI health check"
    return 1
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
    local health_output=""
    if health_output=$(UI_HEALTH_URL="$target" "$PYTHON_BIN" - <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request


def _allow_ws_degraded() -> bool:
    for name in ("LAUNCH_LIVE_ALLOW_WS_DEGRADED", "UI_WS_OPTIONAL"):
        raw = os.environ.get(name)
        if not raw:
            continue
        normalized = raw.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
    return False


ALLOW_WS_DEGRADED = _allow_ws_degraded()


def _status_ok(value, *, allow_ws_degraded=ALLOW_WS_DEGRADED):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return False
        if normalized in {"ok", "running", "ready", "connected", "available", "true"}:
            return True
        if normalized in {"fail", "failed", "error", "inactive", "down", "false", "unavailable"}:
            return False
        if "degrad" in normalized:
            return bool(allow_ws_degraded)
        return True
    if isinstance(value, dict):
        if "ok" in value:
            return _status_ok(value.get("ok"), allow_ws_degraded=allow_ws_degraded)
        if "status" in value:
            return _status_ok(value.get("status"), allow_ws_degraded=allow_ws_degraded)
    return bool(value)


def main() -> int:
    url = os.environ.get("UI_HEALTH_URL")
    if not url:
        print("missing url", end="")
        return 2
    req = urllib.request.Request(url, headers={"User-Agent": "solhunter-launcher"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            status = getattr(resp, "status", 200)
            body = resp.read()
    except urllib.error.HTTPError as exc:
        print(f"http error {exc.code}", end="")
        return 1
    except Exception as exc:  # pragma: no cover - network failure reporting
        print(f"request error: {exc}", end="")
        return 1

    if status >= 400:
        print(f"http status {status}", end="")
        return 1

    text = body.decode("utf-8", errors="replace") if body else ""
    if not text.strip():
        print("empty body", end="")
        return 1
    try:
        payload = json.loads(text)
    except Exception as exc:
        print(f"invalid json: {exc}", end="")
        return 1

    failure_reasons = []
    if not _status_ok(payload.get("ok")):
        failure_reasons.append("ok=false")

    status_block = payload.get("status")
    if isinstance(status_block, dict):
        for key in (
            "event_bus",
            "trading_loop",
            "ui",
            "rl_ws",
            "events_ws",
            "logs_ws",
            "rl_daemon",
            "depth_service",
        ):
            if key not in status_block:
                continue
            value = status_block.get(key)
            if not _status_ok(value):
                failure_reasons.append(f"{key}={value!r}")

    if failure_reasons:
        print("; ".join(failure_reasons), end="")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
PY
    )
    then
      log_info "UI health endpoint responded successfully (url=$target)"
      return 0
    fi
    if [[ -n $health_output ]]; then
      log_warn "UI health payload reported issues: ${health_output}"
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

require_positive_number() {
  local flag=$1
  local value=$2
  if [[ -z $value ]]; then
    echo "$flag requires a value" >&2
    exit 1
  fi
  if ! [[ $value =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    echo "$flag must be a positive numeric value" >&2
    exit 1
  fi
  if command -v bc >/dev/null 2>&1; then
    local bc_result
    bc_result=$(echo "$value > 0" | bc -l 2>/dev/null || true)
    if [[ $bc_result != "1" ]]; then
      echo "$flag must be greater than zero" >&2
      exit 1
    fi
  else
    if ! python3 - "$value" <<'PY'; then
import sys
try:
    value = float(sys.argv[1])
except Exception:
    raise SystemExit(1)
if not value > 0:
    raise SystemExit(1)
PY
      echo "$flag must be greater than zero" >&2
      exit 1
    fi
  fi
}

ENV_FILE=""
MICRO_FLAG=""
CANARY_MODE=0
CANARY_BUDGET=""
CANARY_RISK=""
PREFLIGHT_RUNS=1
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

if [[ $PREFLIGHT_RUNS -lt 1 ]]; then
  echo "--preflight must be at least 1 (set to 1 for the active micro mode, 2 for both micro on/off)" >&2
  exit $EXIT_PREFLIGHT
fi

if [[ $PREFLIGHT_RUNS != "1" && $PREFLIGHT_RUNS != "2" ]]; then
  echo "Invalid --preflight value '$PREFLIGHT_RUNS'; set to 1 for the active micro mode or 2 to cover both micro states." >&2
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

if [[ -n $CANARY_BUDGET ]]; then
  require_positive_number "--budget" "$CANARY_BUDGET"
fi

if [[ -n $CANARY_RISK ]]; then
  require_positive_number "--risk" "$CANARY_RISK"
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
    r"EXAMPLE",
    r"XXXX",
    r"CHANGE_ME",
    r"PLACEHOLDER",
]
patterns = [re.compile(pat, re.IGNORECASE) for pat in pattern_strings]
placeholder_token_pattern = re.compile(r"\$\{([^}]+)\}")
placeholder_prefixes = (
    "REPLACE",
    "CHANGE",
    "FILL",
    "YOUR",
    "EXAMPLE",
    "PLACEHOLDER",
    "TODO",
)
high_risk_value_patterns = [
    re.compile(
        r"""
        ^(?P<scheme>[a-z][a-z0-9+.-]*)://
        (?P<userinfo>[^:@\s]+:[^@/\s]+)@
        """,
        re.IGNORECASE | re.VERBOSE,
    ),
    re.compile(r"-----BEGIN [A-Z0-9 ]+-----"),
    re.compile(r"ssh-(rsa|ed25519|dss) [A-Za-z0-9+/]+=*(?: .*)?", re.IGNORECASE),
]
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
    for token_match in placeholder_token_pattern.finditer(value):
        token = token_match.group(1).strip()
        token_upper = token.upper()
        if any(token_upper.startswith(prefix) for prefix in placeholder_prefixes):
            print(f"placeholder detected for {key}: ${{{token}}}", file=sys.stderr)
            raise SystemExit(1)
    for pat in patterns:
        if pat.search(value):
            print(f"placeholder detected for {key}: {pat.pattern}", file=sys.stderr)
            raise SystemExit(1)
required_reports: list[str] = []
required_exact = [
    ("SOLANA_RPC_URL", "Solana RPC URL"),
    ("SOLANA_WS_URL", "Solana websocket URL"),
    ("REDIS_URL", "Redis endpoint"),
    ("HELIUS_API_KEY", "Helius API key"),
    ("HELIUS_API_KEYS", "Helius API key list"),
    ("HELIUS_API_TOKEN", "Helius auth token"),
    ("HELIUS_RPC_URL", "Helius RPC URL"),
    ("HELIUS_WS_URL", "Helius websocket URL"),
    ("HELIUS_PRICE_RPC_URL", "Helius price RPC URL"),
    ("HELIUS_PRICE_REST_URL", "Helius price REST URL"),
    ("HELIUS_PRICE_BASE_URL", "Helius price base URL"),
    ("BIRDEYE_API_KEY", "Birdeye API key"),
    ("SOLSCAN_API_KEY", "Solscan API key"),
    ("DEX_BASE_URL", "DEX base URL"),
    ("DEX_TESTNET_URL", "DEX testnet URL"),
    ("ORCA_API_URL", "Orca API URL"),
    ("RAYDIUM_API_URL", "Raydium API URL"),
    ("PHOENIX_API_URL", "Phoenix API URL"),
    ("METEORA_API_URL", "Meteora API URL"),
    ("JUPITER_WS_URL", "Jupiter websocket URL"),
    ("JITO_RPC_URL", "Jito RPC URL"),
    ("JITO_AUTH", "Jito bundle auth token"),
    ("JITO_WS_URL", "Jito websocket URL"),
    ("JITO_WS_AUTH", "Jito websocket auth token"),
    ("NEWS_FEEDS", "news feed hooks"),
    ("TWITTER_FEEDS", "Twitter/X feed hooks"),
    ("DISCORD_FEEDS", "Discord feed hooks"),
]
for env_name, description in required_exact:
    candidate = values.get(env_name, "").strip()
    if not candidate:
        required_reports.append(f"{env_name} ({description})")

keypair_candidates = ("KEYPAIR_PATH", "SOLANA_KEYPAIR")
if not any(values.get(name, "").strip() for name in keypair_candidates):
    required_reports.append(
        "keypair pointer missing; set KEYPAIR_PATH or SOLANA_KEYPAIR to a readable keypair JSON"
    )

broker_candidates = (
    "EVENT_BUS_URL",
    "BROKER_URL",
    "BROKER_URLS",
    "BROKER_WS_URLS",
    "BROKER_URLS_JSON",
)
if not any(values.get(name, "").strip() for name in broker_candidates):
    required_reports.append(
        "broker/event bus endpoint missing; set EVENT_BUS_URL or provide BROKER_URL/BROKER_URLS/BROKER_WS_URLS"
    )

if required_reports:
    print("Environment file is missing required entries:", file=sys.stderr)
    for message in required_reports:
        print(f"  - {message}", file=sys.stderr)
    raise SystemExit(1)
manifest = []
for key, value in sorted(values.items()):
    lowered = key.lower()
    value_is_high_risk = any(pat.search(value) for pat in high_risk_value_patterns)
    if value_is_high_risk or any(
        tok in lowered for tok in ("secret", "key", "token", "pass", "private", "pwd")
    ):
        masked = "***"
    else:
        masked = value
    manifest.append(f"{key}={masked}")
print("\n".join(manifest))
PY
}

check_live_keypair_paths() {
  local keypair_report
  if ! keypair_report=$(CONFIG_PATH="$CONFIG_PATH" ENV_FILE_PATH="$ENV_FILE" "$PYTHON_BIN" - <<'PY'
import os
import sys
from pathlib import Path

TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
FALSE_VALUES = {"0", "false", "no", "off", "disabled"}


env_file_dir = None
env_file_path = os.getenv("ENV_FILE_PATH")
if env_file_path:
    try:
        env_path = Path(env_file_path)
    except Exception:  # pragma: no cover - defensive path handling
        env_file_dir = None
    else:
        env_file_dir = env_path.parent


def _parse_bool(value):
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in TRUE_VALUES:
        return True
    if lowered in FALSE_VALUES:
        return False
    return None


def live_trading_requested():
    for name in ("LIVE_TRADING_DISABLED", "PAPER_TRADING", "SHADOW_EXECUTOR_ONLY"):
        state = _parse_bool(os.getenv(name))
        if state:
            return False
    upcoming_mode = os.getenv("UPCOMING_CONTROLLER_MODE")
    if upcoming_mode:
        lowered = upcoming_mode.strip().lower()
        if lowered == "paper":
            return False
        if lowered == "live":
            return True
    for name in ("MODE", "SOLHUNTER_MODE"):
        raw = os.getenv(name)
        if raw:
            lowered = raw.strip().lower()
            if lowered == "paper":
                return False
            if lowered == "live":
                return True
    return True


if not live_trading_requested():
    sys.exit(0)

candidates = {}


def _normalize_candidate(raw_value: str) -> Path:
    candidate = Path(raw_value)
    if env_file_dir and not candidate.is_absolute() and not raw_value.strip().startswith("~"):
        candidate = env_file_dir / candidate
    return candidate.expanduser()


for source in ("KEYPAIR_PATH", "SOLANA_KEYPAIR"):
    raw = os.getenv(source)
    if not raw:
        continue
    path = _normalize_candidate(raw)
    candidates.setdefault(path, set()).add(source)

config_path = os.getenv("CONFIG_PATH")
if config_path:
    try:
        from solhunter_zero.config import load_config
    except Exception as exc:  # pragma: no cover - defensive import guard
        print(f"Failed to import configuration loader: {exc}")
        sys.exit(1)
    try:
        cfg = load_config(config_path)
    except Exception as exc:
        print(f"Failed to load config {config_path}: {exc}")
        sys.exit(1)
    value = cfg.get("solana_keypair")
    if isinstance(value, str) and value.strip():
        candidate = Path(value.strip())
        if config_path and not candidate.is_absolute():
            candidate = Path(config_path).parent / candidate
        path = candidate.expanduser()
        candidates.setdefault(path, set()).add("config.solana_keypair")

if not candidates:
    print(
        "Live trading requires a signing keypair. Set KEYPAIR_PATH to a readable keypair JSON "
        "(for example ~/.config/solana/id.json)."
    )
    sys.exit(1)

issues = []

for path, labels in candidates.items():
    try:
        if not path.exists():
            issues.append((path, labels, "does not exist"))
            continue
        if not path.is_file():
            issues.append((path, labels, "is not a file"))
            continue
        if not os.access(path, os.R_OK):
            issues.append((path, labels, "is not readable"))
            continue
    except OSError as exc:  # pragma: no cover - filesystem edge cases
        issues.append((path, labels, f"is not accessible ({exc})"))

if issues:
    parts = []
    for path, labels, reason in issues:
        sources = ", ".join(sorted(labels))
        parts.append(f"{sources} -> {path} {reason}")
    message = (
        "Live trading requires a readable signing keypair. "
        + "; ".join(parts)
        + ". Set KEYPAIR_PATH to a valid keypair JSON and ensure it exists with chmod 600 permissions."
    )
    print(message)
    sys.exit(1)

sys.exit(0)
PY
  ); then
    if [[ -z $keypair_report ]]; then
      keypair_report="Live trading requires a readable signing keypair. Set KEYPAIR_PATH to a valid keypair JSON and ensure it exists."
    fi
    while IFS= read -r line; do
      if [[ -n $line ]]; then
        log_warn "$line"
      fi
    done <<<"$keypair_report"
    exit $EXIT_KEYS
  fi
}

log_info "Validating environment file $ENV_FILE"
if ! MANIFEST_RAW=$(validate_env_file); then
  exit $EXIT_KEYS
fi

ENV_MANIFEST_PATH="$ROOT_DIR/artifacts/prelaunch/env_manifest.txt"
if mkdir -p "$(dirname "$ENV_MANIFEST_PATH")"; then
  if printf '%s\n' "$MANIFEST_RAW" >"$ENV_MANIFEST_PATH"; then
    log_info "Wrote environment manifest to $ENV_MANIFEST_PATH"
  else
    log_warn "Failed to write environment manifest to $ENV_MANIFEST_PATH"
  fi
else
  log_warn "Failed to create environment manifest directory for $ENV_MANIFEST_PATH"
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

validate_config_path "$CONFIG_PATH"

# Environment files may override PYTHONPATH; ensure the repository root remains
# importable even after sourcing overrides.
ensure_repo_pythonpath

if [[ -n $CONFIG_PATH ]]; then
  export CONFIG_PATH
fi

check_live_keypair_paths

# Ensure Jupiter websocket uses the stats endpoint unless explicitly overridden.
export JUPITER_WS_URL="${JUPITER_WS_URL:-wss://stats.jup.ag/ws}"

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

CONNECTIVITY_SKIP_UI_PROBES_FORCED=0
CONNECTIVITY_SKIP_UI_PROBES_RESTORE=0
CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS_VALUE=""
ui_disabled_reason=""
if [[ ${UI_ENABLED:-1} == "0" ]]; then
  ui_disabled_reason="UI_ENABLED=0"
elif [[ -z ${UI_HOST:-} || -z ${UI_PORT:-} ]]; then
  ui_disabled_reason="UI host/port not configured"
fi
if [[ -n $ui_disabled_reason ]]; then
  CONNECTIVITY_SKIP_UI_PROBES_FORCED=1
  if [[ -n ${CONNECTIVITY_SKIP_UI_PROBES+x} ]]; then
    CONNECTIVITY_SKIP_UI_PROBES_RESTORE=1
    CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS_VALUE="$CONNECTIVITY_SKIP_UI_PROBES"
  fi
  export CONNECTIVITY_SKIP_UI_PROBES=1
  log_info "UI connectivity probes disabled for connectivity check ($ui_disabled_reason)"
fi

log_info "Running connectivity probes"
if ! CONNECTIVITY_REPORT=$(run_connectivity_probes 2>&1); then
  log_warn "Connectivity probes failed:\n$CONNECTIVITY_REPORT"
  if (( CONNECTIVITY_SKIP_UI_PROBES_FORCED )); then
    if (( CONNECTIVITY_SKIP_UI_PROBES_RESTORE )); then
      export CONNECTIVITY_SKIP_UI_PROBES="$CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS_VALUE"
    else
      unset CONNECTIVITY_SKIP_UI_PROBES
    fi
  fi
  exit $EXIT_CONNECTIVITY
fi
while IFS= read -r line; do
  if [[ -n $line ]]; then
    log_info "Connectivity probe: $line"
  fi
done <<<"$CONNECTIVITY_REPORT"
if (( CONNECTIVITY_SKIP_UI_PROBES_FORCED )); then
  if (( CONNECTIVITY_SKIP_UI_PROBES_RESTORE )); then
    export CONNECTIVITY_SKIP_UI_PROBES="$CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS_VALUE"
  else
    unset CONNECTIVITY_SKIP_UI_PROBES
  fi
fi
log_info "Connectivity probes succeeded"

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

log_info "Checking RL daemon health endpoint"
rl_health_payload=""
if ! rl_health_payload=$(rl_health_check); then
  rl_health_detail=$(printf '%s' "$rl_health_payload" | "$PYTHON_BIN" - <<'PY'
import json
import sys

try:
    data = json.loads(sys.stdin.read())
except Exception:
    sys.exit(0)

print(data.get("error") or data.get("message") or "")
PY
  )
  if [[ -n ${rl_health_detail:-} ]]; then
    log_warn "RL daemon health check failed: $rl_health_detail"
  else
    log_warn "RL daemon health check failed"
  fi
  exit $EXIT_HEALTH
fi

rl_health_url=$(printf '%s' "$rl_health_payload" | "$PYTHON_BIN" - <<'PY'
import json
import sys

try:
    data = json.loads(sys.stdin.read())
except Exception:
    sys.exit(0)

print(data.get("url", ""))
PY
)
rl_health_message=$(printf '%s' "$rl_health_payload" | "$PYTHON_BIN" - <<'PY'
import json
import sys

try:
    data = json.loads(sys.stdin.read())
except Exception:
    sys.exit(0)

print(data.get("message", ""))
PY
)
if [[ -n ${rl_health_url:-} ]]; then
  export RL_HEALTH_URL="$rl_health_url"
fi
log_info "RL daemon healthy (${rl_health_message:-ok})"

declare -a CHILD_PIDS=()
START_CONTROLLER_STDERR_STATE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/start_controller.XXXXXX")
register_child() {
  CHILD_PIDS+=("$1")
}

start_controller_stderr_file() {
  local log=$1
  if [[ -z ${START_CONTROLLER_STDERR_STATE_DIR:-} ]]; then
    return 1
  fi
  local key
  key=$(printf '%s' "$log" | LC_ALL=C od -An -tx1 | tr -d ' \n')
  if [[ -z $key ]]; then
    return 1
  fi
  echo "$START_CONTROLLER_STDERR_STATE_DIR/${key}.stderr"
}

print_captured_start_controller_stderr() {
  local log=$1
  local file
  file=$(start_controller_stderr_file "$log") || return 1
  if [[ -f $file && -s $file ]]; then
    cat "$file" >&2 || true
    rm -f "$file" || true
    return 0
  fi
  return 1
}

cleanup() {
  stop_runtime_lock_refresher
  release_runtime_lock
  if [[ -n ${RUNTIME_LOCK_KEY+x} ]]; then
    unset RUNTIME_LOCK_KEY
  fi
  if [[ -n ${RUNTIME_LOCK_TOKEN+x} ]]; then
    unset RUNTIME_LOCK_TOKEN
  fi
  if (( RUNTIME_LOCK_ATTEMPTED == 1 && RUNTIME_LOCK_ACQUIRED == 1 && RUNTIME_FS_LOCK_WRITTEN == 1 )); then
    if [[ -e $RUNTIME_FS_LOCK ]]; then
      local current_payload=""
      if [[ -r $RUNTIME_FS_LOCK ]]; then
        current_payload=$(<"$RUNTIME_FS_LOCK")
      fi
      current_payload=${current_payload//$'\r'/}
      current_payload=${current_payload//$'\n'/}
      if [[ -z $RUNTIME_FS_LOCK_PAYLOAD ]]; then
        log_info "Runtime filesystem lock $RUNTIME_FS_LOCK left in place (ownership unknown)"
      elif [[ $current_payload != "$RUNTIME_FS_LOCK_PAYLOAD" ]]; then
        log_info "Runtime filesystem lock $RUNTIME_FS_LOCK left in place (payload mismatch)"
      elif rm -f "$RUNTIME_FS_LOCK"; then
        log_info "Removed stale runtime filesystem lock ($RUNTIME_FS_LOCK)"
      else
        log_warn "Runtime filesystem lock $RUNTIME_FS_LOCK could not be removed automatically; remove it manually if the runtime is not running"
      fi
    fi
  fi
  if [[ ${LIVE_MODE_ENV_APPLIED:-0} -eq 1 ]]; then
    if [[ ${LIVE_SOLHUNTER_MODE_WAS_SET:-0} -eq 1 ]]; then
      export SOLHUNTER_MODE="$LIVE_SOLHUNTER_MODE_PREVIOUS"
    else
      unset SOLHUNTER_MODE
    fi
    if [[ ${LIVE_MODE_WAS_SET:-0} -eq 1 ]]; then
      export MODE="$LIVE_MODE_PREVIOUS"
    else
      unset MODE
    fi
    LIVE_MODE_ENV_APPLIED=0
  fi
  if [[ -z ${CHILD_PIDS+x} ]]; then
    return
  fi
  for pid in "${CHILD_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [[ -n ${START_CONTROLLER_STDERR_STATE_DIR:-} && -d ${START_CONTROLLER_STDERR_STATE_DIR:-} ]]; then
    rm -rf "$START_CONTROLLER_STDERR_STATE_DIR" || true
  fi
}

trap cleanup EXIT
trap 'cleanup; exit $EXIT_HEALTH' ERR
trap 'cleanup; exit 0' INT TERM

RUNTIME_LOCK_ATTEMPTED=1
acquire_runtime_lock
RUNTIME_LOCK_ACQUIRED=1

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
  local stderr_file
  stderr_file=$(start_controller_stderr_file "$log") || true
  if [[ -n ${stderr_file:-} ]]; then
    mkdir -p "${START_CONTROLLER_STDERR_STATE_DIR:-}" || true
    : >"$stderr_file" || true
  fi
  if ! : > "$log"; then
    if [[ -n ${stderr_file:-} ]]; then
      printf 'Failed to truncate log %s\n' "$log" >>"$stderr_file"
    fi
    return 1
  fi
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
  if [[ -n ${stderr_file:-} ]]; then
    "$PYTHON_BIN" "${args[@]}" >"$log" 2> >(tee -a "$stderr_file" >>"$log") &
  else
    "$PYTHON_BIN" "${args[@]}" >"$log" 2>&1 &
  fi
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
    print_captured_start_controller_stderr "$log" || true
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
  local allow_ws_degraded=0
  local allow_ws_raw="${LAUNCH_LIVE_ALLOW_WS_DEGRADED:-${UI_WS_OPTIONAL:-}}"
  local ui_disabled=0
  if [[ -n ${UI_ENABLED:-} ]]; then
    local normalized="${UI_ENABLED,,}"
    case $normalized in
      0|false|no|off|disabled)
        ui_disabled=1
        ;;
    esac
  fi
  if [[ $ui_disabled -eq 0 && -n ${UI_DISABLE_HTTP_SERVER:-} ]]; then
    local normalized="${UI_DISABLE_HTTP_SERVER,,}"
    case $normalized in
      1|true|yes|on|enabled)
        ui_disabled=1
        ;;
    esac
  fi
  if [[ $ui_disabled -eq 1 ]]; then
    ui_seen=1
    ui_ws_seen=1
  fi
  if [[ -n ${allow_ws_raw:-} ]]; then
    case "${allow_ws_raw,,}" in
      1|true|yes|on|enabled)
        allow_ws_degraded=1
        ;;
    esac
  fi
  while [[ $waited -lt $READY_TIMEOUT ]]; do
    if [[ -n $notify && -f $notify ]]; then
      runtime_seen=1
    fi
    if [[ $ui_seen -eq 0 ]] && grep -q "UI_READY" "$log" 2>/dev/null; then
      ui_seen=1
    fi
    if [[ $ui_ws_seen -eq 0 ]]; then
      local ui_ws_line=""
      ui_ws_line="$(grep -m1 "UI_WS_READY" "$log" 2>/dev/null || true)"
      if [[ -n $ui_ws_line ]]; then
        local ui_ws_status=""
        local ui_ws_detail=""
        if [[ $ui_ws_line =~ status=([^[:space:]]+) ]]; then
          ui_ws_status="${BASH_REMATCH[1]}"
        fi
        if [[ $ui_ws_line =~ detail=(.*) ]]; then
          ui_ws_detail="${BASH_REMATCH[1]}"
        fi
        if [[ -z $ui_ws_status ]]; then
          print_log_excerpt "$log" "UI websocket readiness line missing status: $ui_ws_line"
          exit "$EXIT_HEALTH"
        fi
        case "$ui_ws_status" in
          ok)
            ui_ws_seen=1
            ;;
          degraded)
            if [[ $allow_ws_degraded -eq 1 ]]; then
              ui_ws_seen=1
            else
              local reason="UI websocket readiness reported status=degraded"
              if [[ -n $ui_ws_detail ]]; then
                reason+=" detail=$ui_ws_detail"
              fi
              print_log_excerpt "$log" "$reason"
              exit "$EXIT_HEALTH"
            fi
            ;;
          disabled)
            ui_ws_seen=1
            ;;
          failed)
            local reason="UI websocket readiness reported status=failed"
            if [[ -n $ui_ws_detail ]]; then
              reason+=" detail=$ui_ws_detail"
            fi
            print_log_excerpt "$log" "$reason"
            exit "$EXIT_HEALTH"
            ;;
          *)
            local reason="UI websocket readiness reported status=$ui_ws_status"
            if [[ -n $ui_ws_detail ]]; then
              reason+=" detail=$ui_ws_detail"
            fi
            print_log_excerpt "$log" "$reason"
            exit "$EXIT_HEALTH"
            ;;
        esac
      fi
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
  local -a micro_settings=()

  if [[ $PREFLIGHT_RUNS -eq 1 ]]; then
    micro_settings=("$MICRO_FLAG")
  elif [[ $PREFLIGHT_RUNS -eq 2 ]]; then
    if [[ $MICRO_FLAG == "1" ]]; then
      micro_settings=("1" "0")
    else
      micro_settings=("0" "1")
    fi
  else
    log_warn "Unable to derive preflight micro permutations for PREFLIGHT_RUNS=$PREFLIGHT_RUNS (set to 1 for the active micro mode or 2 to cover both micro states)"
    return 1
  fi

  local expected_runs=${#micro_settings[@]}

  if [[ $PREFLIGHT_RUNS -ne $expected_runs ]]; then
    log_warn "PREFLIGHT_RUNS=$PREFLIGHT_RUNS but expected $expected_runs passes for derived micro permutations"
    return 1
  fi

  local idx=1
  for micro in "${micro_settings[@]}"; do
    log_info "Running preflight suite (mode=$mode micro=$micro pass $idx/$expected_runs)"
    ((idx++))
  done

  local micro_states_string
  micro_states_string=$(printf '%s ' "${micro_settings[@]}")
  micro_states_string=${micro_states_string% }

  local -a env_overrides=(
    "MODE=$mode"
    "MICRO_MODE=${micro_settings[0]}"
    "PREFLIGHT_MICRO_STATES=$micro_states_string"
  )

  if [[ -n ${CONFIG_PATH:-} ]]; then
    env_overrides+=("CONFIG_PATH=$CONFIG_PATH")
  fi

  if [[ -n ${SOLHUNTER_MODE:-} ]]; then
    env_overrides+=("SOLHUNTER_MODE=$SOLHUNTER_MODE")
  fi

  if ! env "${env_overrides[@]}" bash "$ROOT_DIR/scripts/preflight/run_all.sh"; then
    return 1
  fi
}

log_info "Running preflight suite"
runtime_lock_ttl_check "preflight" || true
if ! run_preflight "paper"; then
  log_warn "Preflight suite failed"
  exit $EXIT_PREFLIGHT
fi
log_info "Preflight suite completed successfully"

kill "$PAPER_PID" >/dev/null 2>&1 || true
wait "$PAPER_PID" 2>/dev/null || true
log_info "Paper runtime stopped after preflight"

# Preserve the caller's CONNECTIVITY_SKIP_UI_PROBES configuration while the
# paper runtime is offline for the soak.
CONNECTIVITY_SKIP_UI_PROBES_WAS_SET=0
if [[ -n ${CONNECTIVITY_SKIP_UI_PROBES+x} ]]; then
  CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS="$CONNECTIVITY_SKIP_UI_PROBES"
  CONNECTIVITY_SKIP_UI_PROBES_WAS_SET=1
fi
export CONNECTIVITY_SKIP_UI_PROBES=1
log_info "Paper runtime offline; skipping UI connectivity probes during soak"
log_info "Starting connectivity soak for ${SOAK_DURATION}s"
runtime_lock_ttl_check "soak" || true
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

runtime_lock_ttl_check "post-soak" || true
log_info "Connectivity soak complete: $SOAK_RESULT"
if (( CONNECTIVITY_SKIP_UI_PROBES_WAS_SET )); then
  export CONNECTIVITY_SKIP_UI_PROBES="$CONNECTIVITY_SKIP_UI_PROBES_PREVIOUS"
else
  unset CONNECTIVITY_SKIP_UI_PROBES
fi
log_info "UI connectivity probes re-enabled for live launch"

LIVE_SOLHUNTER_MODE_WAS_SET=0
if [[ -n ${SOLHUNTER_MODE+x} ]]; then
  LIVE_SOLHUNTER_MODE_PREVIOUS="$SOLHUNTER_MODE"
  LIVE_SOLHUNTER_MODE_WAS_SET=1
fi
LIVE_MODE_WAS_SET=0
if [[ -n ${MODE+x} ]]; then
  LIVE_MODE_PREVIOUS="$MODE"
  LIVE_MODE_WAS_SET=1
fi
export SOLHUNTER_MODE=live
export MODE=live
LIVE_MODE_ENV_APPLIED=1
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
GO_NO_GO="GO/NO-GO: Keys OK | Services OK | Preflight PASSED (${PREFLIGHT_RUNS}/${PREFLIGHT_RUNS}) | Soak PASSED | MODE=live | MICRO=${micro_label}${canary_label}"
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
