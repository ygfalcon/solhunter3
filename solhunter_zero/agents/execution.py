from __future__ import annotations

import asyncio
import base64
import os
from typing import Dict, Any, List

import aiohttp
from ..http import get_session
from ..event_bus import subscription
from ..dynamic_limit import _target_concurrency, _step_limit

from . import BaseAgent
from ..exchange import (
    place_order_async,
    DEX_BASE_URL,
    VENUE_URLS,
    _sign_transaction,
    resolve_swap_endpoint,
)
from ..execution import EventExecutor
from ..depth_client import submit_raw_tx
from ..portfolio import Portfolio


class ExecutionAgent(BaseAgent):
    """Submit orders with simple rate limiting."""

    name = "execution"

    def __init__(
        self,
        *,
        rate_limit: float = 1.0,
        concurrency: int = 1,
        testnet: bool = False,
        dry_run: bool = False,
        keypair=None,
        retries: int = 1,
        depth_service: bool = False,
        priority_fees: list[float] | None = None,
        priority_rpc: list[str] | None = None,
        min_rate: float | None = None,
        max_rate: float | None = None,
    ):
        self.rate_limit = rate_limit
        self.min_rate = float(min_rate) if min_rate is not None else 0.0
        self.max_rate = float(max_rate) if max_rate is not None else rate_limit
        self._base_concurrency = max(1, int(concurrency))
        self.testnet = testnet
        self.dry_run = dry_run
        self.keypair = keypair
        self.retries = retries
        self._sem = asyncio.Semaphore(concurrency)
        self._rate_lock = asyncio.Lock()
        self._last = 0.0
        self.depth_service = depth_service
        self._executors: Dict[str, EventExecutor] = {}
        self.priority_fees = list(priority_fees) if priority_fees else None
        self.priority_rpc = list(priority_rpc) if priority_rpc else None
        self._cpu_usage = 0.0
        self._cpu_smoothed = 0.0
        self._smoothing = float(os.getenv("CONCURRENCY_SMOOTHING", "0.2") or 0.2)
        self._resource_subs = [
            subscription("system_metrics", self._on_resource_update),
            subscription("resource_update", self._on_resource_update),
            subscription("system_metrics_combined", self._on_resource_update),
        ]
        for sub in self._resource_subs:
            sub.__enter__()

    def _on_resource_update(self, payload: Any) -> None:
        """Update resource usage and adjust concurrency and rate limit."""
        cpu = getattr(payload, "cpu", None)
        if isinstance(payload, dict):
            cpu = payload.get("cpu", cpu)
        if cpu is None:
            return
        try:
            self._cpu_usage = float(cpu)
            if self._cpu_smoothed:
                self._cpu_smoothed = (
                    self._smoothing * self._cpu_usage
                    + (1 - self._smoothing) * self._cpu_smoothed
                )
            else:
                self._cpu_smoothed = self._cpu_usage
        except Exception:
            return
        frac = max(0.0, min(1.0, self._cpu_smoothed / 100.0))
        target = _target_concurrency(self._cpu_smoothed, self._base_concurrency, 0.0, 100.0)
        conc = _step_limit(self._sem._value, target, self._base_concurrency)
        if conc != self._sem._value:
            self._sem = asyncio.Semaphore(conc)
        self.rate_limit = self.min_rate + (self.max_rate - self.min_rate) * frac

    async def _create_signed_tx(
        self,
        token: str,
        side: str,
        amount: float,
        price: float,
        endpoint: str,
        *,
        priority_fee: int | None = None,
    ) -> str | None:
        """Return a signed transaction for ``token`` using ``endpoint``."""

        payload = {
            "token": token,
            "side": side,
            "amount": amount,
            "price": price,
            "cluster": "devnet" if self.testnet else "mainnet-beta",
        }

        session = await get_session()
        try:
            async with session.post(endpoint, json=payload, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            return None

        tx_b64 = data.get("swapTransaction")
        if not tx_b64:
            return None

        if self.depth_service:
            from ..depth_client import prepare_signed_tx

            return await prepare_signed_tx(tx_b64, priority_fee=priority_fee)

        if self.keypair is None:
            return None

        tx = _sign_transaction(tx_b64, self.keypair)
        return base64.b64encode(bytes(tx)).decode()

    def add_executor(self, token: str, executor: EventExecutor) -> None:
        """Register an :class:`EventExecutor` for ``token``."""

        self._executors[token] = executor

    def close(self) -> None:
        """Unsubscribe from resource updates."""
        if hasattr(self, "_resource_subs"):
            for sub in self._resource_subs:
                sub.__exit__(None, None, None)

    async def execute(self, action: Dict[str, Any]) -> Any:
        async with self._sem:
            async with self._rate_lock:
                now = asyncio.get_event_loop().time()
                delay = self.rate_limit - (now - self._last)
                if delay > 0:
                    await asyncio.sleep(delay)
                self._last = asyncio.get_event_loop().time()

            # Read current mempool transaction rate
            from ..depth_client import snapshot

            _depth, tx_rate = snapshot(action["token"])

            priority_fee: int | None = None
            pri_idx = int(action.get("priority", 0))
            if self.priority_fees and 0 <= pri_idx < len(self.priority_fees):
                from ..gas import adjust_priority_fee

                priority_fee = int(
                    adjust_priority_fee(tx_rate) * self.priority_fees[pri_idx]
                )

            venue = str(action.get("venue", "")).strip().lower()
            venues_raw = action.get("venues")

            base_entries: list[tuple[str | None, str]] = []
            normalized_venues: list[str] | None = None

            custom_base = action.get("base_url")
            primary_base: str | None = None
            if custom_base:
                primary_base = str(custom_base)
                base_entries.append((None, primary_base))

            if isinstance(venues_raw, list):
                normalized: list[str] = []
                for entry in venues_raw:
                    text = str(entry).strip()
                    if not text:
                        continue
                    if "://" in text:
                        normalized.append(text)
                        base_entries.append((None, text))
                    else:
                        norm = text.lower()
                        normalized.append(norm)
                        base_entries.append((norm, str(VENUE_URLS.get(norm, text))))
                normalized_venues = normalized or None
            else:
                normalized_venues = None

            if primary_base is None:
                if base_entries:
                    primary_base = base_entries[0][1]
                else:
                    base_value = str(VENUE_URLS.get(venue, DEX_BASE_URL))
                    base_entries.append((venue or None, base_value))
                    primary_base = base_value

            endpoints: list[str] = []
            seen_endpoints: set[str] = set()
            for hint, base in base_entries:
                if not base:
                    continue
                endpoint = resolve_swap_endpoint(str(base), venue=hint)
                if endpoint and endpoint not in seen_endpoints:
                    endpoints.append(endpoint)
                    seen_endpoints.add(endpoint)

            if not endpoints:
                primary_base = str(DEX_BASE_URL)
                endpoints.append(resolve_swap_endpoint(primary_base))

            if self.depth_service:
                for endpoint in endpoints:
                    tx = await self._create_signed_tx(
                        action["token"],
                        action["side"],
                        action.get("amount", 0.0),
                        action.get("price", 0.0),
                        endpoint,
                        priority_fee=priority_fee,
                    )
                    if tx:
                        execer = self._executors.get(action["token"])
                        if execer:
                            await execer.enqueue(tx)
                        else:
                            await submit_raw_tx(
                                tx,
                                priority_rpc=self.priority_rpc,
                                priority_fee=priority_fee,
                            )
                        return {"queued": True}
                return None

            amount = action.get("amount", 0.0)
            pri_idx = int(action.get("priority", 0))
            if (
                self.priority_fees
                and 0 <= pri_idx < len(self.priority_fees)
                and self.priority_rpc
            ):
                from ..gas import get_priority_fee_estimate

                fee = await get_priority_fee_estimate(self.priority_rpc)
                amount = max(0.0, amount - fee * self.priority_fees[pri_idx])

            return await place_order_async(
                action["token"],
                action["side"],
                amount,
                action.get("price", 0.0),
                testnet=self.testnet,
                dry_run=self.dry_run,
                keypair=self.keypair,
                base_url=primary_base,
                venues=normalized_venues,
                max_retries=action.get("retries", self.retries),
                timeout=action.get("timeout"),
            )

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        # Execution agent does not propose trades itself
        return []
