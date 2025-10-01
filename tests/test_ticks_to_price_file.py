import json
import pytest
import paper


def test_ticks_to_price_file_filters_invalid_entries(tmp_path):
    ticks = [
        {"price": "1.0", "timestamp": 1},
        {"price": "bad", "timestamp": 2},
        {"timestamp": 3},
        {"price": None, "timestamp": 4},
        {"price": 5.0, "timestamp": 5},
    ]
    dataset = paper._ticks_to_price_file(ticks)
    data = json.loads(dataset.read_text())
    assert data == [
        {"date": "1", "price": 1.0},
        {"date": "5", "price": 5.0},
    ]
    dataset.unlink()


@pytest.mark.parametrize(
    "ticks",
    [
        [],
        [{"timestamp": 1}, {"price": "bad", "timestamp": 2}],
    ],
)
def test_ticks_to_price_file_raises_on_empty_or_invalid(ticks):
    with pytest.raises(ValueError, match="tick dataset is empty"):
        paper._ticks_to_price_file(ticks)
