from __future__ import annotations

import os
from typing import AsyncGenerator, Iterable, List

from . import BaseAgent
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from ..mempool_scanner import stream_ranked_mempool_tokens_with_depth

stream_ranked_mempool_tokens_with_depth = None
from ..portfolio import Portfolio
from ..arbitrage import _prepare_service_tx
from ..exchange import DEX_BASE_URL
from ..mev_executor import MEVExecutor
from ..util import sanitize_priority_urls


class MempoolSniperAgent(BaseAgent):
    """Listen to ranked mempool events and immediately submit swap bundles."""

    name = "mempool_sniper"

    def __init__(
        self,
        mempool_threshold: float | None = None,
        *,
        bundle_size: int | None = None,
        amount: float = 1.0,
        priority_rpc: Iterable[str] | None = None,
        jito_rpc_url: str | None = None,
        jito_auth: str | None = None,
        base_url: str = DEX_BASE_URL,
    ) -> None:
        if mempool_threshold is None:
            mempool_threshold = float(
                os.getenv("MEMPOOL_THRESHOLD", "0") or 0.0
            )
        if bundle_size is None:
            bundle_size = int(os.getenv("BUNDLE_SIZE", "1") or 1)
        self.mempool_threshold = float(mempool_threshold)
        self.bundle_size = int(bundle_size)
        self.amount = float(amount)
        cleaned_priority = sanitize_priority_urls(priority_rpc)
        self.priority_rpc = cleaned_priority or None
        self.jito_rpc_url = jito_rpc_url or os.getenv("JITO_RPC_URL")
        self.jito_auth = jito_auth or os.getenv("JITO_AUTH")
        self.base_url = base_url

    async def listen(
        self,
        rpc_url: str,
        *,
        suffix: str | None = None,
        keywords: Iterable[str] | None = None,
        include_pools: bool = True,
    ) -> AsyncGenerator[str, None]:
        """Yield tokens that triggered a buy and submit bundles."""

        from ..mempool_scanner import (
            stream_ranked_mempool_tokens_with_depth as _default_stream,
        )

        stream_fn = stream_ranked_mempool_tokens_with_depth or _default_stream

        tx_buffer: List[str] = []
        async for event in stream_fn(
            rpc_url,
            suffix=suffix,
            keywords=keywords,
            include_pools=include_pools,
            threshold=self.mempool_threshold,
        ):
            token = event["address"]
            score = float(event.get("combined_score", event.get("score", 0.0)))
            liquidity = float(event.get("liquidity", 0.0))
            if liquidity > 0 or score >= self.mempool_threshold:
                tx = await _prepare_service_tx(
                    token,
                    "buy",
                    self.amount,
                    0.0,
                    self.base_url,
                )
                if tx:
                    tx_buffer.append(tx)
                    if len(tx_buffer) >= self.bundle_size:
                        mev = MEVExecutor(
                            token,
                            priority_rpc=self.priority_rpc,
                            jito_rpc_url=self.jito_rpc_url,
                            jito_auth=self.jito_auth,
                        )
                        await mev.submit_bundle(list(tx_buffer))
                        tx_buffer.clear()
                yield token

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ):
        # Trading decisions are handled in ``listen``
        return []
