import asyncio
import os
import tempfile
import unittest
from types import SimpleNamespace

from solhunter_zero import rl_training
from solhunter_zero.rl_daemon import RLDaemon


def _make_trade(token: str, direction: str, price: float, amount: float, ts: float = 0.0):
    return SimpleNamespace(
        token=token,
        direction=direction,
        price=price,
        amount=amount,
        timestamp=ts,
    )


def _make_snapshot(token: str, price: float, *, depth: float = 1.0, imbalance: float = 0.0, ts: float = 0.0):
    return SimpleNamespace(
        token=token,
        price=price,
        depth=depth,
        imbalance=imbalance,
        slippage=0.0,
        tx_rate=0.0,
        volume=0.0,
        timestamp=ts,
    )


class RLSequentialPipelineTest(unittest.TestCase):
    def test_dataset_builds_expected_shapes(self):
        trades = [_make_trade("T", "buy", 1.0, 2.0, ts=1.0), _make_trade("T", "sell", 1.2, 1.5, ts=2.0)]
        snaps = [_make_snapshot("T", 1.0, ts=0.5), _make_snapshot("T", 1.1, ts=1.5)]
        dataset = rl_training._TradeDataset(trades, snaps)
        self.assertEqual(len(dataset), 2)
        state, action, reward = dataset[0]
        self.assertEqual(state.shape[0], 9)
        self.assertEqual(action.item(), 0)
        self.assertLess(reward.item(), 0.0)

    def test_fit_creates_checkpoint(self):
        trades = [_make_trade("X", "sell", 2.0, 1.0, ts=1.0)]
        snaps = [_make_snapshot("X", 1.9, ts=1.0)]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "model.pt")
            summary = rl_training.fit(trades, snaps, model_path=path, algo="ppo")
            self.assertTrue(os.path.exists(path))
            self.assertGreaterEqual(summary.samples, 1)

    def test_rl_daemon_train_cycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            mem_path = os.path.join(tmp, "mem.db")
            data_path = os.path.join(tmp, "offline.db")
            model_path = os.path.join(tmp, "model.pt")

            daemon = RLDaemon(
                memory_path=f"sqlite:///{mem_path}",
                data_path=data_path,
                model_path=model_path,
                interval=0.1,
            )

            async def _seed_and_train():
                await daemon.memory.log_trade(
                    token="Y",
                    direction="sell",
                    amount=1.0,
                    price=1.5,
                )
                await daemon.offline.log_snapshot("Y", 1.4, depth=1.0, imbalance=0.0)
                await daemon.train()

            asyncio.run(_seed_and_train())
            self.assertTrue(os.path.exists(model_path))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

