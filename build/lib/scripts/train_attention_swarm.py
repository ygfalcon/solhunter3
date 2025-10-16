import argparse
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.agents.attention_swarm import (
    train_attention_swarm,
    save_model,
)


def main() -> None:
    p = argparse.ArgumentParser(description="Train attention-based swarm model")
    p.add_argument("--db", default="memory.db", help="AdvancedMemory database")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--window", type=int, default=50, help="Trades per window")
    p.add_argument("--seq-len", type=int, default=5, help="ROI history length")
    p.add_argument("--epochs", type=int, default=10)
    args = p.parse_args()

    mem = AdvancedMemory(f"sqlite:///{args.db}")
    trades = mem.list_trades()
    agents = sorted({t.reason or "" for t in trades})
    model = train_attention_swarm(
        mem,
        agents,
        window=args.window,
        seq_len=args.seq_len,
        epochs=args.epochs,
    )
    save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
