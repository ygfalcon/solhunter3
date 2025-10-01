import pytest
torch = pytest.importorskip("torch")
from solhunter_zero.models.gnn import train_route_gnn, rank_routes


def test_rank_routes():
    routes = [["a", "b"], ["a", "c"], ["b", "c"]]
    profits = [0.1, 0.5, 0.0]
    model = train_route_gnn(routes, profits, epochs=50, lr=0.05)
    idx = rank_routes(model, [["a", "b"], ["a", "c"]])
    assert idx == 1

