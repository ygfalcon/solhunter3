from __future__ import annotations

import pytest

import paper


def test_preset_with_ticks_raises_value_error():
    """--preset cannot be combined with --ticks."""
    with pytest.raises(
        ValueError, match="Cannot combine --preset with --ticks/--fetch-live"
    ):
        paper.run(["--preset", "short", "--ticks", "file.json"])


def test_preset_with_fetch_live_raises_value_error():
    """--preset cannot be combined with --fetch-live."""
    with pytest.raises(
        ValueError, match="Cannot combine --preset with --ticks/--fetch-live"
    ):
        paper.run(["--preset", "short", "--fetch-live"])
