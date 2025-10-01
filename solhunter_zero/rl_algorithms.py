from __future__ import annotations

try:
    import torch
    from torch import nn
except ImportError as exc:  # pragma: no cover - optional dependency
    class _TorchStub:
        class Module:
            def __init__(self, *a, **k) -> None:
                raise ImportError("torch is required for rl_algorithms")

        def __getattr__(self, name):
            raise ImportError("torch is required for rl_algorithms")

    torch = nn = _TorchStub()  # type: ignore


class _A3C(nn.Module):
    """Minimal actor-critic network used for A3C style training."""

    def __init__(self, input_size: int = 9, hidden_size: int = 32) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )
        self.loss_fn = nn.MSELoss()

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # pragma: no cover - simple
        return self.actor(x)


class _DDPG(nn.Module):
    """Simple actor-critic network for DDPG."""

    def __init__(self, input_size: int = 9, hidden_size: int = 32) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
            nn.Tanh(),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size + 1, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )
        self.loss_fn = nn.MSELoss()

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # pragma: no cover - simple
        return self.actor(x)


class TransformerPolicy(nn.Module):
    """Simple transformer policy network for PPO/A3C."""

    def __init__(
        self,
        input_size: int = 8,
        hidden_size: int = 32,
        num_layers: int = 2,
        num_heads: int = 4,
        clip_epsilon: float = 0.2,
    ) -> None:
        super().__init__()
        self.embed = nn.Linear(input_size, hidden_size)
        enc_layer = nn.TransformerEncoderLayer(
            hidden_size, nhead=num_heads, dim_feedforward=hidden_size * 2
        )
        self.encoder = nn.TransformerEncoder(enc_layer, num_layers)
        self.actor = nn.Linear(hidden_size, 2)
        self.critic = nn.Linear(hidden_size, 1)
        self.clip_epsilon = clip_epsilon
        self.loss_fn = nn.MSELoss()

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # pragma: no cover - simple
        if x.dim() == 2:
            x = x.unsqueeze(1)
        x = self.embed(x)
        out = self.encoder(x)[:, -1]
        return self.actor(out)
