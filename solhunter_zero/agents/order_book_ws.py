"""Legacy shim exposing order book websocket helpers for agents.

This module re-exports the functionality from :mod:`solhunter_zero.order_book_ws`
so that older import paths under :mod:`solhunter_zero.agents` continue to work
without modification.
"""

from __future__ import annotations

from .. import order_book_ws as _order_book_ws

__all__ = [
    name
    for name in vars(_order_book_ws)
    if not (name.startswith("__") and name.endswith("__"))
]

globals().update({name: getattr(_order_book_ws, name) for name in __all__})
