import argparse
from solhunter_zero.agents.sac_agent import SACAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory


def main() -> None:
    p = argparse.ArgumentParser(description="Train SAC trading agent")
    p.add_argument("--memory", default="sqlite:///memory.db")
    p.add_argument("--data", default="sqlite:///offline_data.db")
    p.add_argument("--out", required=True, help="Path to save trained model")
    args = p.parse_args()

    mem = Memory(args.memory)
    mem_agent = MemoryAgent(mem)
    agent = SACAgent(memory_agent=mem_agent, data_url=args.data, model_path=args.out)
    agent.train()
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
