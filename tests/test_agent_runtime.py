import asyncio

from solhunter_zero.agents.runtime.agent_runtime import AgentRuntime
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.token_aliases import canonical_mint


class _DummyManager:
    pass


def test_on_tokens_normalizes_and_deduplicates(monkeypatch):
    runtime = AgentRuntime(manager=_DummyManager(), portfolio=Portfolio(path=None))
    runtime._running = True

    observed: list[str] = []

    async def fake_evaluate(token: str) -> None:
        observed.append(token)

    monkeypatch.setattr(runtime, "_evaluate_and_publish", fake_evaluate)

    alias = "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v"
    canonical = canonical_mint(alias)

    payload = [
        {"mint": alias},
        {"token": f"  {alias}  "},
        alias,
        alias.lower(),
        {"address": alias},
    ]

    async def runner() -> None:
        runtime._on_tokens(payload)
        tasks = list(runtime._tasks)
        if tasks:
            await asyncio.gather(*tasks)

    asyncio.run(runner())

    assert observed == [canonical]
    assert runtime._tokens == {canonical}
    assert not runtime._tasks
