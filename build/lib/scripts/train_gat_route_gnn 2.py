import argparse
import asyncio

from solhunter_zero.memory import Memory
from solhunter_zero.models import save_route_gnn, train_route_gnn

async def _load_data(mem: Memory, token: str | None):
    trades = await mem.list_trades(token=token)
    routes = []
    profits = []
    for t in trades:
        if not t.reason:
            continue
        parts = [p.strip() for p in str(t.reason).split("->") if p.strip()]
        if len(parts) <= 1:
            continue
        routes.append(parts)
        value = float(t.amount) * float(t.price)
        profits.append(value if t.direction == "sell" else -value)
    return routes, profits


def main() -> None:
    p = argparse.ArgumentParser(description="Train route GNN from trade history")
    p.add_argument("--db", default="memory.db", help="Memory database path")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    p.add_argument("--gat", action="store_true", help="Use attention variant")
    args = p.parse_args()

    mem = Memory(f"sqlite:///{args.db}")
    routes, profits = asyncio.run(_load_data(mem, args.token))
    if len(routes) <= 1:
        raise SystemExit("not enough route data")
    model = train_route_gnn(routes, profits, epochs=args.epochs, gat=args.gat)
    save_route_gnn(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
