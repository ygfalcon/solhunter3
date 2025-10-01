import asyncio
import importlib.util

import pytest

pytest.importorskip("ray")
torch_spec = importlib.util.find_spec("torch")
if torch_spec is None:
    pytest.skip("torch not installed", allow_module_level=True)

from solhunter_zero.ray_training import RayTraining
from solhunter_zero.offline_data import OfflineData


@pytest.mark.asyncio
async def test_ray_training_runs(tmp_path):
    db = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db)
    await data.log_snapshot("tok", 1.0, 1.0, imbalance=0.0, total_depth=1.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)
    await data.log_trade("tok", "sell", 1.0, 1.1)

    model_path = tmp_path / "ppo_model.pt"
    trainer = RayTraining(db_url=db, model_path=model_path, num_actors=1)
    trainer.train()
    trainer.close()
    assert model_path.exists()
