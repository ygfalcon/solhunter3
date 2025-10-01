import argparse
from solhunter_zero.agents.ppo_agent import PPOAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory


def main() -> None:
    parser = argparse.ArgumentParser(description="Train PPO trading agent")
    parser.add_argument("--memory", default="sqlite:///memory.db")
    parser.add_argument("--data", default="sqlite:///offline_data.db")
    parser.add_argument("--out", required=True, help="Path to save trained model")
    args = parser.parse_args()

    mem = Memory(args.memory)
    mem_agent = MemoryAgent(mem)
    agent = PPOAgent(memory_agent=mem_agent, data_url=args.data, model_path=args.out)
    agent.train()
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
