from __future__ import annotations

import logging
import threading
import aiohttp
import xml.etree.ElementTree as ET
from typing import Iterable, List, Sequence

from .http import get_session
from .async_utils import run_async

try:
    from transformers import pipeline
except ImportError as exc:  # pragma: no cover - optional dependency
    def pipeline(*args, **kwargs):  # type: ignore
        raise ImportError(
            "transformers is required for sentiment analysis features"
        )

logger = logging.getLogger(__name__)

# Cache for the HuggingFace pipeline
_pipeline: "pipeline | None" = None
_pipe_lock = threading.Lock()
HEADLINE_LIMIT = 200  # keep inference fast/cost-effective


def get_pipeline() -> pipeline:
    """Return the shared DistilBERT sentiment pipeline."""
    global _pipeline
    if _pipeline is None:
        with _pipe_lock:
            if _pipeline is None:
                _pipeline = pipeline(
                    "sentiment-analysis",
                    model="distilbert-base-uncased-finetuned-sst-2-english",
                )
    return _pipeline

async def fetch_headlines_async(
    feed_urls: Iterable[str],
    allowed: Iterable[str] | None = None,
    *,
    twitter_urls: Iterable[str] | None = None,
    discord_urls: Iterable[str] | None = None,
) -> List[str]:
    """Return headlines and posts from the configured URLs.

    Any feeds not included in ``allowed`` are ignored.  If ``allowed`` is
    ``None``, all URLs are permitted.
    """
    allowed_set = {url for url in allowed} if allowed is not None else None
    headlines: List[str] = []
    session = await get_session()

    for url in feed_urls:
        if allowed_set is not None and url not in allowed_set:
            logger.warning("Blocked RSS feed: %s", url)
            continue
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                text = await resp.text()
        except aiohttp.ClientError as exc:  # pragma: no cover - network errors
            logger.warning("Failed to fetch feed %s: %s", url, exc)
            continue
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:  # pragma: no cover - bad xml
            logger.warning("Failed to parse feed %s: %s", url, exc)
            continue
        for item in root.findall(".//item/title"):
            if item.text:
                headlines.append(item.text.strip())
        for entry in root.findall(".//{http://www.w3.org/2005/Atom}entry"):
            t = entry.find("{http://www.w3.org/2005/Atom}title")
            if t is not None and t.text:
                headlines.append(t.text.strip())

    for url in twitter_urls or []:
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except (aiohttp.ClientError, ValueError) as exc:  # pragma: no cover - network errors
            logger.warning("Failed to fetch twitter feed %s: %s", url, exc)
            continue
        posts = data.get("posts", []) or data.get("data", [])
        for post in posts:
            if isinstance(post, str):
                headlines.append(post.strip())
            elif isinstance(post, dict):
                txt = post.get("text") or post.get("title")
                if isinstance(txt, str):
                    headlines.append(txt.strip())

    for url in discord_urls or []:
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except (aiohttp.ClientError, ValueError) as exc:  # pragma: no cover - network errors
            logger.warning("Failed to fetch discord feed %s: %s", url, exc)
            continue
        messages = data.get("messages", []) or data.get("data", [])
        for msg in messages:
            if isinstance(msg, str):
                headlines.append(msg.strip())
            elif isinstance(msg, dict):
                txt = msg.get("content") or msg.get("message") or msg.get("title")
                if isinstance(txt, str):
                    headlines.append(txt.strip())

    seen = set()
    out: List[str] = []
    for h in headlines:
        t = (h or "").strip()
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= HEADLINE_LIMIT:
            break
    return out


def fetch_headlines(
    feed_urls: Iterable[str],
    allowed: Iterable[str] | None = None,
    *,
    twitter_urls: Iterable[str] | None = None,
    discord_urls: Iterable[str] | None = None,
) -> List[str]:
    """Synchronous wrapper for :func:`fetch_headlines_async`."""
    return run_async(
        lambda: fetch_headlines_async(
            feed_urls,
            allowed,
            twitter_urls=twitter_urls,
            discord_urls=discord_urls,
        )
    )

def _label_to_score(label: str, score: float) -> float:
    lbl = (label or "").lower()
    s = float(score or 0.0)
    return -s if lbl.startswith("neg") else s


def compute_sentiment(text: str) -> float:
    """Return a sentiment score for ``text`` between -1 and 1 using DistilBERT."""
    if not text.strip():
        return 0.0
    clf = get_pipeline()
    try:
        result = clf(text)[0]
    except Exception as exc:  # pragma: no cover
        logger.warning("Sentiment model failure: %s", exc)
        return 0.0
    return _label_to_score(result.get("label", ""), float(result.get("score", 0.0)))


def compute_sentiment_batch(texts: Sequence[str]) -> float:
    """Mean sentiment over multiple texts (faster and less biased than concatenation)."""
    texts = [t for t in texts if t and t.strip()]
    if not texts:
        return 0.0
    clf = get_pipeline()
    try:
        results = clf(list(texts), truncation=True)
    except Exception as exc:  # pragma: no cover
        logger.warning("Sentiment model failure: %s", exc)
        return 0.0
    if not results:
        return 0.0
    scores = [
        _label_to_score(r.get("label", ""), float(r.get("score", 0.0)))
        for r in results
    ]
    avg = sum(scores) / len(scores)
    return max(-1.0, min(1.0, avg))

async def fetch_sentiment_async(
    feed_urls: Iterable[str],
    allowed: Iterable[str] | None = None,
    *,
    twitter_urls: Iterable[str] | None = None,
    discord_urls: Iterable[str] | None = None,
) -> float:
    """Return overall sentiment for provided sources."""
    headlines = await fetch_headlines_async(
        feed_urls,
        allowed,
        twitter_urls=twitter_urls,
        discord_urls=discord_urls,
    )
    if not headlines:
        return 0.0
    return compute_sentiment_batch(headlines)


def fetch_sentiment(
    feed_urls: Iterable[str],
    allowed: Iterable[str] | None = None,
    *,
    twitter_urls: Iterable[str] | None = None,
    discord_urls: Iterable[str] | None = None,
) -> float:
    """Synchronous wrapper for :func:`fetch_sentiment_async`."""
    try:
        return run_async(
            lambda: fetch_sentiment_async(
                feed_urls,
                allowed,
                twitter_urls=twitter_urls,
                discord_urls=discord_urls,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.debug("Sentiment fetch failed: %s", exc)
        return 0.0
