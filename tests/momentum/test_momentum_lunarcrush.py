import pytest

from solhunter_zero.golden_pipeline.momentum import MomentumAgent


@pytest.mark.asyncio
async def test_lunarcrush_fallback_populates_social(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = MomentumAgent(pipeline=None, publish=lambda *_: None, config=None)
    agent._lunarcrush_enabled = True
    agent._lunarcrush_host = "lunarcrush.test"
    agent._lunarcrush_url_template = "https://lunarcrush.test/{symbol}"
    agent._lunarcrush_api_key = None

    async def fake_request_json(self, url, *, host, params=None, headers=None):
        assert host == "lunarcrush.test"
        return {"data": [{"galaxy_score": 72.0}]}

    async def fake_nitter(self, symbol: str) -> tuple[float | None, str | None]:
        return None, None

    monkeypatch.setattr(
        agent,
        "_request_json",
        fake_request_json.__get__(agent, MomentumAgent),
    )
    monkeypatch.setattr(
        agent,
        "_fetch_nitter_mentions",
        fake_nitter.__get__(agent, MomentumAgent),
    )

    payload = await agent._fetch_social_symbol("SOL", [])
    assert payload["social_source"] == "lunarcrush"
    assert payload["social_sentiment"] == pytest.approx(0.72)
