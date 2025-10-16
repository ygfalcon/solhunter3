import asyncio
from pathlib import Path
from typing import Any

try:
    import torch
except ImportError as exc:  # pragma: no cover - optional dependency
    class _TorchStub:
        def __getattr__(self, name):
            raise ImportError("torch is required for ray_training")

    torch = _TorchStub()  # type: ignore

try:
    import ray
    from ray.rllib.algorithms.ppo import PPOConfig
    from ray.rllib.algorithms.dqn import DQNConfig
    import gym
except Exception:  # pragma: no cover - ray optional
    ray = None

from .rl_training import _TradeDataset
from .offline_data import OfflineData


class _OfflineDatasetEnv(gym.Env):
    """RLlib environment that replays offline samples sequentially."""

    def __init__(self, config: dict[str, Any]):
        self.dataset = config["dataset"]
        self._index = 0
        self.action_space = gym.spaces.Discrete(2)
        self.observation_space = gym.spaces.Box(
            low=-float("inf"), high=float("inf"), shape=(9,), dtype=float
        )

    def reset(self, *, seed: int | None = None, options: dict[str, Any] | None = None):
        self._index = 0
        if len(self.dataset) == 0:
            return [0.0] * 9
        state, _, _ = self.dataset[0]
        return state.numpy().tolist()

    def step(self, action: int):
        state, exp_act, reward = self.dataset[self._index]
        done = self._index >= len(self.dataset) - 1
        self._index += 1
        if not done:
            next_state = self.dataset[self._index][0].numpy().tolist()
        else:
            next_state = state.numpy().tolist()
        r = float(reward)
        if int(action) != int(exp_act):
            r = -abs(r)
        return next_state, r, done, {}


def _build_trainer(dataset: _TradeDataset, algo: str):
    if algo == "dqn":
        cfg = DQNConfig().framework("torch")
    else:
        cfg = PPOConfig().framework("torch")
    cfg = cfg.environment(_OfflineDatasetEnv, env_config={"dataset": dataset})
    cfg = cfg.rollouts(num_rollout_workers=0)
    return cfg.build()


async def _load_dataset(db_url: str) -> _TradeDataset:
    data = OfflineData(db_url)
    trades = await data.list_trades()
    snaps = await data.list_snapshots()
    return _TradeDataset(trades, snaps)


@ray.remote
class _RayWorker:
    def __init__(self, *, db_url: str, model_path: str | Path, algo: str):
        dataset = asyncio.run(_load_dataset(db_url))
        self.model_path = Path(model_path)
        self.trainer = _build_trainer(dataset, algo)

    def train(self) -> Any:
        result = self.trainer.train()
        state = self.trainer.get_policy().model.state_dict()
        torch.save(state, self.model_path)
        return result

    def close(self) -> None:
        self.trainer.stop()


class RayTraining:
    """Train RL models in parallel using Ray and RLlib."""

    def __init__(
        self,
        *,
        db_url: str = "sqlite:///offline_data.db",
        model_path: str | Path = "ppo_model.pt",
        algo: str = "ppo",
        num_actors: int = 1,
    ) -> None:
        if ray is None:  # pragma: no cover - missing dependency
            raise RuntimeError("ray is not available")
        ray.init(ignore_reinit_error=True)
        opts = dict(db_url=db_url, model_path=str(model_path), algo=algo)
        self.actors = [_RayWorker.remote(**opts) for _ in range(max(1, int(num_actors)))]

    def train(self) -> None:
        ray.get([a.train.remote() for a in self.actors])

    def close(self) -> None:
        ray.get([a.close.remote() for a in self.actors])
        ray.shutdown()
