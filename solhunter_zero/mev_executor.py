from __future__ import annotations

from typing import Sequence, List, Optional

import aiohttp
from .http import get_session

from .depth_client import submit_raw_tx, snapshot, DEPTH_SERVICE_SOCKET
from .gas import adjust_priority_fee
from .util import sanitize_priority_urls


class MEVExecutor:
    """Bundle swap transactions and submit them with priority fees."""

    def __init__(
        self,
        token: str,
        *,
        priority_rpc: List[str] | None = None,
        socket_path: str = DEPTH_SERVICE_SOCKET,
        jito_rpc_url: str | None = None,
        jito_auth: str | None = None,
    ) -> None:
        self.token = token
        cleaned_priority = sanitize_priority_urls(priority_rpc)
        self.priority_rpc = cleaned_priority or None
        self.socket_path = socket_path
        self.jito_rpc_url = jito_rpc_url
        self.jito_auth = jito_auth

    async def _submit_jito_bundle(self, txs: Sequence[str]) -> List[Optional[str]]:
        """Send ``txs`` to the Jito block-engine API."""

        if not self.jito_rpc_url:
            return []
        headers = {}
        if self.jito_auth:
            headers["Authorization"] = self.jito_auth
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": {"transactions": list(txs)},
        }
        session = await get_session()
        try:
            async with session.post(
                self.jito_rpc_url, json=payload, headers=headers, timeout=10
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            return [None for _ in txs]
        result = data.get("result")
        if isinstance(result, list):
            return [str(s) for s in result]
        return [None for _ in txs]
    async def submit_bundle(self, txs: Sequence[str]) -> List[Optional[str]]:
        """Submit ``txs`` with a compute unit price based on mempool rate."""
        if self.jito_rpc_url:
            return await self._submit_jito_bundle(txs)

        _, rate = snapshot(self.token)
        priority_fee = adjust_priority_fee(rate)
        sigs = []
        for tx in txs:
            sig = await submit_raw_tx(
                tx,
                priority_rpc=self.priority_rpc,
                priority_fee=priority_fee,
            )
            sigs.append(sig)
        return sigs
