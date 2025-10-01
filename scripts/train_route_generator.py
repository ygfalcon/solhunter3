import argparse
import asyncio

from solhunter_zero.memory import Memory
from solhunter_zero.models import save_route_generator, train_route_generator


async def _load_routes(mem: Memory, token: str | None):
    trades = await mem.list_trades(token=token)
    routes = []
    for t in trades:
        if t.reason:
            parts = [p.strip() for p in str(t.reason).split("->") if p.strip()]
            if len(parts) > 1:
                routes.append(parts)
    return routes


def main() -> None:
    p = argparse.ArgumentParser(description="Train route generator from trade history")
    p.add_argument("--db", default="memory.db", help="Memory database path")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    args = p.parse_args()

    mem = Memory(f"sqlite:///{args.db}")
    routes = asyncio.run(_load_routes(mem, args.token))
    if len(routes) <= 1:
        raise SystemExit("not enough route data")
    model = train_route_generator(routes, epochs=args.epochs)
    save_route_generator(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
