from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


STUB_REDIS = """
import json
import os
import time

STATE_PATH = os.environ.get("REDIS_STATE_PATH")
if not STATE_PATH:
    raise RuntimeError("REDIS_STATE_PATH not set")


def _load_state():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return {"store": {}}


def _dump_state(data):
    with open(STATE_PATH, "w", encoding="utf-8") as handle:
        json.dump(data, handle)


class Redis:
    def __init__(self, url: str) -> None:
        self.url = url

    @classmethod
    def from_url(cls, url: str, socket_timeout: float | None = None):  # noqa: D401
        return cls(url)

    def get(self, key: str):  # noqa: D401
        data = _load_state()
        item = data["store"].get(key)
        if not item:
            return None
        expires_at = item.get("expires_at")
        if expires_at is not None and expires_at <= time.time():
            data["store"].pop(key, None)
            _dump_state(data)
            return None
        return item["value"].encode("utf-8")

    def set(self, key: str, value: str, nx: bool = False, ex: int | None = None):  # noqa: D401
        data = _load_state()
        now = time.time()
        item = data["store"].get(key)
        if item:
            expires_at = item.get("expires_at")
            if expires_at is not None and expires_at <= now:
                data["store"].pop(key, None)
                item = None
        if nx and item is not None:
            return False
        expires_at = None
        if ex is not None:
            expires_at = now + ex
        data["store"][key] = {
            "value": value,
            "expires_at": expires_at,
            "ex": ex,
            "stored_at": now,
        }
        _dump_state(data)
        return True

    def delete(self, key: str):  # noqa: D401
        data = _load_state()
        removed = data["store"].pop(key, None)
        _dump_state(data)
        return 1 if removed else 0

    def ttl(self, key: str):  # noqa: D401
        data = _load_state()
        item = data["store"].get(key)
        if not item:
            return -2
        expires_at = item.get("expires_at")
        if expires_at is None:
            return -1
        remaining = expires_at - time.time()
        if remaining <= 0:
            data["store"].pop(key, None)
            _dump_state(data)
            return -2
        return int(remaining)

    def eval(self, script, numkeys, key, token, *args):  # noqa: D401
        data = _load_state()
        item = data["store"].get(key)
        if not item:
            return -2 if "set" in script else 0
        try:
            payload = json.loads(item["value"])
        except Exception:
            payload = {}
        if "redis.call('set'" in script:
            if payload.get("token") != token:
                return -3
            try:
                ttl_seconds = int(args[0]) if args else 60
            except Exception:
                ttl_seconds = 60
            payload["ts"] = float(args[1]) if len(args) > 1 else time.time()
            now = time.time()
            data["store"][key] = {
                "value": json.dumps(payload),
                "expires_at": now + ttl_seconds,
                "ex": ttl_seconds,
                "stored_at": now,
            }
            _dump_state(data)
            return int(self.ttl(key))
        if payload.get("token") == token:
            data["store"].pop(key, None)
            _dump_state(data)
            return 1
        return 0
"""



def _extract_function(source: str, name: str) -> str:
    marker = f"{name}() {{"
    start = source.find(marker)
    if start == -1:
        raise ValueError(f"Function {name} not found in launch_live.sh")
    brace_depth = 0
    end = None
    for idx, char in enumerate(source[start:], start=start):
        if char == "{":
            brace_depth += 1
        elif char == "}":
            brace_depth -= 1
            if brace_depth == 0:
                end = idx + 1
                break
    if end is None:
        raise ValueError(f"Failed to parse function {name} from launch_live.sh")
    return source[start:end] + "\n"


def _write_stub_redis(tmp_path: Path) -> Path:
    stub_root = tmp_path / "redis_stub"
    package_dir = stub_root / "redis"
    package_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text(STUB_REDIS, encoding="utf-8")
    return stub_root


def _base_env(tmp_path: Path, stub_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    python_path = env.get("PYTHONPATH")
    if python_path:
        python_path = f"{stub_root}{os.pathsep}{python_path}"
    else:
        python_path = str(stub_root)
    env.update(
        {
            "PYTHON_BIN": sys.executable,
            "PYTHONPATH": python_path,
            "REDIS_STATE_PATH": str(tmp_path / "redis_state.json"),
        }
    )
    return env


def test_acquire_runtime_lock_sets_ttl(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "acquire_runtime_lock")
    )

    stub_root = _write_stub_redis(tmp_path)
    env = _base_env(tmp_path, stub_root)

    bash_script = "set -euo pipefail\n" + functions + "acquire_runtime_lock\n"

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    assert completed.returncode == 0

    state_path = Path(env["REDIS_STATE_PATH"])
    assert state_path.exists()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["store"], "expected runtime lock entry"
    key, entry = next(iter(state["store"].items()))
    assert key.startswith("solhunter:runtime:lock:")
    assert entry["ex"] == 60
    assert pytest.approx(60, rel=0.05) == entry["expires_at"] - entry["stored_at"]
    assert entry["expires_at"] > time.time()


def test_acquire_runtime_lock_takeover_on_host_mismatch(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "acquire_runtime_lock")
    )

    stub_root = _write_stub_redis(tmp_path)
    env = _base_env(tmp_path, stub_root)
    state_path = Path(env["REDIS_STATE_PATH"])

    key = "solhunter:runtime:lock:solhunter-events-v3"
    remote_payload = {
        "pid": 9999,
        "channel": "solhunter-events-v3",
        "host": "remote-host",
        "token": "remote-token",
        "ts": time.time(),
    }
    state = {
        "store": {
            key: {
                "value": json.dumps(remote_payload),
                "expires_at": time.time() + 90,
                "ex": 60,
                "stored_at": time.time(),
            }
        }
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")

    bash_script = "set -euo pipefail\n" + functions + "acquire_runtime_lock\n"

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    assert "Taking over runtime lock" in completed.stderr

    updated = json.loads(state_path.read_text(encoding="utf-8"))
    assert key in updated["store"]
    entry = updated["store"][key]
    payload = json.loads(entry["value"])
    assert payload["host"] == socket.gethostname()
    assert payload["token"] != "remote-token"


def test_release_runtime_lock_requires_matching_token(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    acquire_functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "acquire_runtime_lock")
    )
    release_function = _extract_function(source, "release_runtime_lock")

    stub_root = _write_stub_redis(tmp_path)
    env = _base_env(tmp_path, stub_root)

    acquire_script = "set -euo pipefail\n" + acquire_functions + "acquire_runtime_lock\n"

    subprocess.run(
        ["bash", "-c", acquire_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    state_path = Path(env["REDIS_STATE_PATH"])
    state = json.loads(state_path.read_text(encoding="utf-8"))
    key, entry = next(iter(state["store"].items()))
    payload = json.loads(entry["value"])
    correct_token = payload["token"]

    bad_env = env.copy()
    bad_env.update(
        {
            "RUNTIME_LOCK_KEY": key,
            "RUNTIME_LOCK_TOKEN": f"{correct_token}-bad",
        }
    )
    release_script = "set -euo pipefail\n" + release_function + "release_runtime_lock\n"

    subprocess.run(
        ["bash", "-c", release_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=bad_env,
        check=True,
    )

    still_locked = json.loads(state_path.read_text(encoding="utf-8"))
    assert key in still_locked["store"]

    good_env = env.copy()
    good_env.update(
        {
            "RUNTIME_LOCK_KEY": key,
            "RUNTIME_LOCK_TOKEN": correct_token,
        }
    )

    subprocess.run(
        ["bash", "-c", release_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=good_env,
        check=True,
    )

    final_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert key not in final_state["store"]


def test_runtime_lock_refresher_keeps_ttl(tmp_path: Path) -> None:
    script_path = REPO_ROOT / "scripts" / "launch_live.sh"
    source = script_path.read_text()
    functions = (
        _extract_function(source, "timestamp")
        + _extract_function(source, "log_info")
        + _extract_function(source, "log_warn")
        + _extract_function(source, "acquire_runtime_lock")
        + _extract_function(source, "release_runtime_lock")
        + _extract_function(source, "start_runtime_lock_refresher")
        + _extract_function(source, "stop_runtime_lock_refresher")
        + _extract_function(source, "runtime_lock_ttl_check")
    )

    stub_root = _write_stub_redis(tmp_path)
    env = _base_env(tmp_path, stub_root)
    env.update(
        {
            "RUNTIME_LOCK_REFRESH_INTERVAL": "1",
            "RUNTIME_LOCK_TTL_SECONDS": "5",
        }
    )

    bash_script = "\n".join(
        [
            "set -euo pipefail",
            functions,
            "acquire_runtime_lock",
            "start_runtime_lock_refresher",
            "sleep 2",
            "runtime_lock_ttl_check preflight",
            "runtime_lock_ttl_check soak",
            "stop_runtime_lock_refresher",
            "release_runtime_lock",
        ]
    )

    completed = subprocess.run(
        ["bash", "-c", bash_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    stdout = completed.stdout
    assert "Runtime lock TTL during preflight" in stdout
    assert "Runtime lock TTL during soak" in stdout

    state_path = Path(env["REDIS_STATE_PATH"])
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["store"] == {}
