import argparse
import asyncio

from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
import solhunter_zero.device as device


async def main() -> None:
    parser = argparse.ArgumentParser(description="Continuous DQN training")
    parser.add_argument("--memory", default="sqlite:///memory.db")
    parser.add_argument("--model", default="dqn_model.pt")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--replay", default="sqlite:///replay.db")
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()

    dev = device.get_default_device(args.device)

    mem = Memory(args.memory)
    mem_agent = MemoryAgent(mem)
    agent = DQNAgent(memory_agent=mem_agent, model_path=args.model, replay_url=args.replay, device=dev)
    agent.start_online_learning(interval=args.interval)
    await asyncio.Event().wait()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
