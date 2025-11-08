from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import logging
import asyncio
import threading
import urllib.parse
from pathlib import Path

import websockets

from . import wallet, data_sync, main as main_module, event_bus
from .event_bus import DEFAULT_WS_URL
from .config import (
    CONFIG_DIR,
    get_active_config_name,
    load_config,
    apply_env_overrides,
    initialize_event_bus,
    get_remote_event_bus_urls,
)
from .service_launcher import (
    start_depth_service,
    start_rl_daemon,
    wait_for_depth_ws,
)
from .paths import ROOT
PROCS: list[subprocess.Popen] = []

_EVENT_LOOP: asyncio.AbstractEventLoop | None = None
_EVENT_THREAD: threading.Thread | None = None


def shutdown_event_bus() -> None:
    """Stop the event bus thread and its event loop if running."""
    global _EVENT_LOOP, _EVENT_THREAD
    if _EVENT_LOOP is None:
        return
    try:
        fut = asyncio.run_coroutine_threadsafe(
            event_bus.stop_ws_server(), _EVENT_LOOP
        )
        try:
            fut.result(timeout=5)
        except Exception:  # pragma: no cover - best effort cleanup
            pass
    finally:
        try:
            _EVENT_LOOP.call_soon_threadsafe(_EVENT_LOOP.stop)
        except Exception:
            pass
        if _EVENT_THREAD is not None:
            _EVENT_THREAD.join(timeout=1)
    _EVENT_LOOP = None
    _EVENT_THREAD = None


def _stop_all(*_: object, exit_code: int = 0) -> None:
    data_sync.stop_scheduler()
    shutdown_event_bus()
    for p in PROCS:
        if p.poll() is None:
            p.terminate()
    deadline = time.time() + 5
    for p in PROCS:
        if p.poll() is None:
            try:
                p.wait(deadline - time.time())
            except Exception:
                p.kill()
    sys.exit(exit_code)


def _ensure_keypair() -> None:
    from solhunter_zero.startup_checks import ensure_wallet_cli, run_quick_setup

    ensure_wallet_cli()
    try:
        info = wallet.setup_default_keypair()
    except Exception as exc:
        print(f"Wallet interaction failed: {exc}", file=sys.stderr)
        print("Attempting quick non-interactive setup...", file=sys.stderr)
        try:
            run_quick_setup()
            info = wallet.setup_default_keypair()
        except Exception as exc2:
            print(
                f"Wallet interaction failed: {exc2}\n"
                "Run 'solhunter-wallet' manually or set the MNEMONIC "
                "environment variable.",
                file=sys.stderr,
            )
            raise SystemExit(1)

    path = os.path.join(wallet.KEYPAIR_DIR, info.name + ".json")
    os.environ["KEYPAIR_PATH"] = path
    print(f"Using keypair: {info.name}")


def _get_config() -> tuple[str | None, dict]:
    name = get_active_config_name()
    cfg_path: str | None = None
    if name:
        path = os.path.join(CONFIG_DIR, name)
        if os.path.isfile(path):
            cfg_path = path
    else:
        preset = Path(ROOT / "config" / "default.toml")
        if preset.is_file():
            cfg_path = str(preset)
    cfg: dict = {}
    if cfg_path:
        cfg = apply_env_overrides(load_config(cfg_path))
    return cfg_path, cfg


def _start_event_bus(url: str) -> None:
    """Launch the websocket event bus locally and ensure it is reachable."""
    global _EVENT_LOOP, _EVENT_THREAD
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 8766
    loop = asyncio.new_event_loop()
    _EVENT_LOOP = loop

    async def runner() -> None:
        await event_bus.start_ws_server(host, port)

    def _run() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner())
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True)
    _EVENT_THREAD = thread
    thread.start()

    async def _check() -> str:
        listen_host, listen_port = event_bus.get_ws_address()
        local_url = f"ws://{listen_host}:{listen_port}"
        async with websockets.connect(local_url):
            return local_url

    deadline = time.time() + 5
    local_url: str | None = None
    while time.time() < deadline:
        try:
            local_url = asyncio.run(asyncio.wait_for(_check(), 1))
            break
        except Exception:
            time.sleep(0.1)
    if not local_url:
        raise RuntimeError(f"Event bus failed to start at {url}")

    os.environ["EVENT_BUS_URL"] = local_url


def _maybe_start_event_bus(cfg: dict) -> None:
    """Start a local event bus if ``EVENT_BUS_URL`` points to localhost."""
    if os.getenv("EVENT_BUS_DISABLE_LOCAL", "").strip().lower() in {"1", "true", "yes"}:
        remote_urls = get_remote_event_bus_urls(cfg)
        if remote_urls:
            os.environ["EVENT_BUS_URL"] = remote_urls[0]
            os.environ["BROKER_WS_URLS"] = ",".join(remote_urls)
        else:
            os.environ.pop("EVENT_BUS_URL", None)
            os.environ.pop("BROKER_WS_URLS", None)
        return
    url = os.getenv("EVENT_BUS_URL") or cfg.get("event_bus_url")
    if not url:
        _start_event_bus(DEFAULT_WS_URL)
        return
    host = urllib.parse.urlparse(url).hostname
    if host and host in {"localhost", "0.0.0.0", "127.0.0.1"}:
        try:
            _start_event_bus(url)
        except Exception as exc:
            print(f"Could not start event bus at {url}: {exc}", file=sys.stderr)
            sys.exit(1)


def main() -> None:
    signal.signal(signal.SIGINT, _stop_all)
    signal.signal(signal.SIGTERM, _stop_all)

    logging.basicConfig(level=logging.INFO)
    from . import device

    device.initialize_gpu()

    _ensure_keypair()

    cfg_path, cfg = _get_config()
    interval = float(
        cfg.get(
            "offline_data_interval",
            os.getenv("OFFLINE_DATA_INTERVAL", "3600"),
        )
    )
    db_path_val = cfg.get("rl_db_path", "offline_data.db")
    db_path = Path(db_path_val)
    if not db_path.is_absolute():
        db_path = ROOT / db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(db_path, "a"):
            pass
    except OSError as exc:
        print(f"Cannot write to {db_path}: {exc}", file=sys.stderr)
        sys.exit(1)
    offline = os.getenv("SOLHUNTER_OFFLINE", "").lower() in {"1", "true", "yes"}
    if offline:
        logging.info("Offline mode detected; skipping offline data sync scheduler")
    else:
        data_sync.start_scheduler(interval=interval, db_path=str(db_path))
    _maybe_start_event_bus(cfg)
    initialize_event_bus()

    try:
        depth_proc = start_depth_service(cfg_path)
        PROCS.append(depth_proc)
        PROCS.append(start_rl_daemon())

        addr = os.getenv("DEPTH_WS_ADDR", "127.0.0.1")
        port = int(os.getenv("DEPTH_WS_PORT", "8766"))
        deadline = time.monotonic() + 30.0
        wait_for_depth_ws(addr, port, deadline, depth_proc)
    except Exception as exc:
        logging.error(str(exc))
        _stop_all(exit_code=1)

    exit_code = 0
    try:
        main_module.run_auto()
    except Exception:
        logging.exception("run_auto failed")
        exit_code = 1
    finally:
        _stop_all(exit_code=exit_code)


if __name__ == "__main__":  # pragma: no cover - manual invocation
    main()
