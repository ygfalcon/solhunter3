from solders.pubkey import Pubkey

from solhunter_zero.agents import discovery
from solhunter_zero.scanner_common import STATIC_FALLBACK_TOKENS


def test_static_fallback_tokens_are_valid_base58():
    seen = set()
    for mint in STATIC_FALLBACK_TOKENS:
        # Raises ValueError if the mint is not valid base58 / length
        Pubkey.from_string(mint)
        seen.add(mint)

    assert len(seen) == len(STATIC_FALLBACK_TOKENS)
    assert list(STATIC_FALLBACK_TOKENS) == discovery._STATIC_FALLBACK
