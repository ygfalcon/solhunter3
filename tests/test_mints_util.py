from __future__ import annotations

import os

from base58 import b58encode

from solhunter_zero.util.mints import clean_mints


def test_valid_dedup():
    mint = b58encode(os.urandom(32)).decode()
    out = clean_mints([mint, mint])
    assert out.valid == [mint]
    assert out.dropped == []


def test_extract_from_url():
    mint = "4Nd1mQ9Vqv9Lh6g7a7dC1W2xXqgX8i5T1n5p7v9q5Cdu"
    url = f"https://solscan.io/token/{mint}?cluster=mainnet-beta"
    out = clean_mints(url)
    assert out.valid == [mint]
    assert out.dropped == []


def test_invalid_chars_reason():
    out = clean_mints(["O0Il" * 10])
    assert out.valid == []
    assert out.reasons[out.dropped[0]] == "invalid_chars"
