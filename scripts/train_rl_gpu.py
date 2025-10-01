import argparse
import asyncio

from solhunter_zero.rl_training import RLTraining


async def main() -> None:
    p = argparse.ArgumentParser(description="Periodic RL training on GPU")
    p.add_argument("--db", default="sqlite:///offline_data.db")
    p.add_argument("--model", default="ppo_model.pt")
    p.add_argument("--algo", choices=["ppo", "dqn", "a3c", "ddpg"], default="ppo")
    p.add_argument("--interval", type=float, default=3600.0)
    args = p.parse_args()

    trainer = RLTraining(db_url=args.db, model_path=args.model, algo=args.algo)
    trainer.start_periodic_retraining(interval=args.interval)

    await asyncio.Event().wait()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

