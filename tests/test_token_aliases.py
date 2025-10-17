from __future__ import annotations

import importlib
import json

import pytest

import solhunter_zero.token_aliases as token_aliases


@pytest.fixture()
def reload_aliases(monkeypatch):
    """Reload ``token_aliases`` with an optional runtime alias overlay."""

    def _reload(mapping: dict[str, str] | None = None):
        if mapping is None:
            monkeypatch.delenv("TOKEN_ALIAS_JSON", raising=False)
        else:
            monkeypatch.setenv("TOKEN_ALIAS_JSON", json.dumps(mapping))
        return importlib.reload(token_aliases)

    yield _reload
    monkeypatch.delenv("TOKEN_ALIAS_JSON", raising=False)
    importlib.reload(token_aliases)


def test_canonical_mint_normalizes_whitespace_and_zero_width(reload_aliases):
    module = reload_aliases()
    raw = " \u200bJUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v\ufeff "
    assert (
        module.canonical_mint(raw)
        == "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
    )


def test_canonical_mint_resolves_runtime_alias_chain(reload_aliases):
    alias_address = "111111111111111111111111111111112"
    module = reload_aliases({alias_address: "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v"})
    assert (
        module.canonical_mint(alias_address)
        == "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
    )


def test_normalize_mint_or_none_validates_output(reload_aliases):
    module = reload_aliases()
    valid = module.normalize_mint_or_none(
        "  JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN  "
    )
    assert valid == "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
    assert module.normalize_mint_or_none("O0Il" * 10) is None
    assert module.normalize_mint_or_none(None) is None
