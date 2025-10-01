import argparse
import time
from pathlib import Path
from itertools import product

import torch
from torch import nn
from torch.utils.data import DataLoader

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.rl_daemon import _PPO, _TradeDataset, _metrics
import solhunter_zero.device as device


def _train(model: _PPO, data: _TradeDataset, device: torch.device, lr: float) -> None:
    loader = DataLoader(data, batch_size=32, shuffle=True)
    opt = torch.optim.Adam(list(model.actor.parameters()) + list(model.critic.parameters()), lr=lr)
    model.train()
    loss_fn = nn.MSELoss()
    for _ in range(3):
        for states, actions, rewards in loader:
            states = states.to(device)
            actions = actions.to(device)
            rewards = rewards.to(device)
            dist = torch.distributions.Categorical(logits=model.actor(states))
            log_probs = dist.log_prob(actions)
            values = model.critic(states).squeeze()
            with torch.no_grad():
                advantages = rewards - values
                returns = rewards
            ratio = torch.exp(log_probs - log_probs.detach())
            s1 = ratio * advantages
            s2 = torch.clamp(ratio, 1 - model.clip_epsilon, 1 + model.clip_epsilon) * advantages
            actor_loss = -torch.min(s1, s2).mean()
            critic_loss = loss_fn(values, returns)
            loss = actor_loss + 0.5 * critic_loss
            opt.zero_grad()
            loss.backward()
            opt.step()


def _evaluate(model: _PPO, data: _TradeDataset, device: torch.device) -> float:
    loader = DataLoader(data, batch_size=32)
    loss_fn = nn.MSELoss()
    model.eval()
    total = 0.0
    with torch.no_grad():
        for states, actions, rewards in loader:
            states = states.to(device)
            actions = actions.to(device)
            rewards = rewards.to(device)
            dist = torch.distributions.Categorical(logits=model.actor(states))
            log_probs = dist.log_prob(actions)
            values = model.critic(states).squeeze()
            advantages = rewards - values
            ratio = torch.exp(log_probs - log_probs)
            s1 = ratio * advantages
            s2 = torch.clamp(ratio, 1 - model.clip_epsilon, 1 + model.clip_epsilon) * advantages
            actor_loss = -torch.min(s1, s2).mean()
            critic_loss = loss_fn(values, rewards)
            total += float(actor_loss + 0.5 * critic_loss)
    return total / max(len(loader), 1)


def tune(db_path: str, model_path: Path, device: torch.device) -> None:
    data = OfflineData(f"sqlite:///{db_path}")
    trades = data.list_trades()
    snaps = data.list_snapshots()
    metrics = _metrics(trades, snaps)
    dataset = _TradeDataset(trades, snaps, metrics)
    if len(dataset) == 0:
        return
    params = list(product([1e-4, 3e-4, 1e-3], [0.1, 0.2, 0.3]))
    best_loss = float("inf")
    best_state = None
    for lr, clip in params:
        model = _PPO(clip_epsilon=clip).to(device)
        _train(model, dataset, device, lr)
        loss = _evaluate(model, dataset, device)
        if loss < best_loss:
            best_loss = loss
            best_state = model.state_dict()
    if best_state is not None:
        torch.save(best_state, model_path)


def main() -> None:
    p = argparse.ArgumentParser(description="Periodic PPO tuning from offline data")
    p.add_argument("--db", default="offline_data.db")
    p.add_argument("--model", default="ppo_model.pt")
    p.add_argument("--interval", type=float, default=3600.0)
    p.add_argument("--device", default="auto")
    args = p.parse_args()

    dev = device.get_default_device(args.device)

    model_path = Path(args.model)
    while True:
        try:
            tune(args.db, model_path, dev)
        except Exception as exc:  # pragma: no cover - logging
            print(f"tuning failed: {exc}")
        time.sleep(args.interval)


if __name__ == "__main__":  # pragma: no cover
    main()
