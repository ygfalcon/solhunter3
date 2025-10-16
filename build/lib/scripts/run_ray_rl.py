import argparse

from solhunter_zero.ray_training import RayTraining


def main() -> None:
    p = argparse.ArgumentParser(description="Run Ray-based RL training")
    p.add_argument("--db", default="sqlite:///offline_data.db")
    p.add_argument("--model", default="ppo_model.pt")
    p.add_argument("--algo", choices=["ppo", "dqn"], default="ppo")
    p.add_argument("--actors", type=int, default=1)
    args = p.parse_args()

    trainer = RayTraining(db_url=args.db, model_path=args.model, algo=args.algo, num_actors=args.actors)
    trainer.train()
    trainer.close()


if __name__ == "__main__":  # pragma: no cover
    main()
