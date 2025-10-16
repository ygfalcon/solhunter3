from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List

class BaseMemory(ABC):
    """Abstract interface for memory implementations."""

    @abstractmethod
    def log_trade(self, *args, **kwargs) -> int | None:
        """Record a trade and optionally return its identifier."""
        raise NotImplementedError

    @abstractmethod
    def list_trades(
        self,
        *,
        token: str | None = None,
        limit: int | None = None,
        since_id: int | None = None,
    ) -> List[Any]:
        """Return logged trades with optional filtering."""
        raise NotImplementedError

    def search(self, query: str, k: int = 5) -> List[Any]:
        """Optional semantic search over stored trades."""
        raise NotImplementedError
