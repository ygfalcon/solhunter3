"""Compatibility bridge for :mod:`solhunter_zero.agents.event_bus`.

Legacy startup scripts and a handful of third-party extensions still import
``solhunter_zero.agents.event_bus`` even though the implementation lives at the
package root (``solhunter_zero.event_bus``).  After the package layout change
those imports started to fail which prevented :class:`~solhunter_zero.agent_manager.AgentManager`
from initialising during startup.

To keep the public surface area stable we provide a very small shim that proxies
all attribute lookups to the canonical module.  Using ``__getattr__`` keeps the
proxy light-weight while ensuring ``from solhunter_zero.agents.event_bus import
publish`` continues to work as expected.
"""

from __future__ import annotations

from .. import event_bus as _event_bus

# Mirror the public API exposed by ``solhunter_zero.event_bus`` when available
# so ``from module import *`` behaves the same.
__all__ = getattr(_event_bus, "__all__", [])


def __getattr__(name: str):
    return getattr(_event_bus, name)


def __dir__() -> list[str]:  # pragma: no cover - simple helper
    local = set(globals()) - {"_event_bus"}
    return sorted(local | set(dir(_event_bus)))

