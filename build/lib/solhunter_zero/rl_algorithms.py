from __future__ import annotations

from typing import Optional, Tuple

try:
    import torch
    from torch import Tensor, nn
    from torch.distributions import Categorical
except ImportError:  # pragma: no cover - optional dependency
    class _TorchStub:
        class Module:
            def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - stub
                raise ImportError(
                    "torch is required for rl_algorithms; please install the torch package"
                )

        def __getattr__(self, name):  # pragma: no cover - stub
            raise ImportError(
                "torch is required for rl_algorithms; please install the torch package"
            )

    torch = _TorchStub()  # type: ignore
    nn = _TorchStub()  # type: ignore
    Tensor = object  # type: ignore[misc,assignment]
    Categorical = object  # type: ignore[misc,assignment]


def _orthogonal_init(module: nn.Module) -> None:
    """Apply orthogonal initialization to linear layers."""

    if isinstance(module, nn.Linear):
        nn.init.orthogonal_(module.weight)
        if module.bias is not None:
            nn.init.zeros_(module.bias)


class A3CPolicy(nn.Module):
    """Minimal actor-critic network used for A3C/PPO style training."""

    def __init__(self, input_size: int = 9, hidden_size: int = 32) -> None:
        super().__init__()
        self.backbone = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
        )
        self.policy_head = nn.Linear(hidden_size, 2)
        self.value_head = nn.Linear(hidden_size, 1)
        self.apply(_orthogonal_init)

    def forward(self, x: Tensor) -> Tuple[Tensor, Tensor]:
        features = self.backbone(x)
        logits = self.policy_head(features)
        value = self.value_head(features).squeeze(-1)
        return logits, value

    def act(self, x: Tensor, deterministic: bool = False) -> Tuple[Tensor, Tensor, Tensor]:
        logits, value = self.forward(x)
        dist = Categorical(logits=logits)
        if deterministic:
            action = torch.argmax(logits, dim=-1)
        else:
            action = dist.sample()
        log_prob = dist.log_prob(action)
        return action, log_prob, value

    def evaluate(
        self, x: Tensor, actions: Tensor
    ) -> Tuple[Tensor, Tensor, Tensor, Optional[Tensor]]:
        logits, value = self.forward(x)
        dist = Categorical(logits=logits)
        log_prob = dist.log_prob(actions)
        entropy = dist.entropy()
        return log_prob, entropy, value, None


class DDPGPolicy(nn.Module):
    """Simple actor-critic network for deterministic policy gradients."""

    def __init__(self, input_size: int = 9, hidden_size: int = 32) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
            nn.Tanh(),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size + 1, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )
        self.apply(_orthogonal_init)

    def forward(self, x: Tensor) -> Tuple[Tensor, Tensor]:
        action = self.actor(x)
        critic_input = torch.cat([x, action], dim=-1)
        value = self.critic(critic_input).squeeze(-1)
        return action, value

    def act(
        self,
        x: Tensor,
        deterministic: bool = True,
        noise_std: float = 0.0,
    ) -> Tuple[Tensor, Optional[Tensor], Tensor]:
        action, value = self.forward(x)
        if not deterministic and noise_std > 0:
            noise = torch.randn_like(action) * noise_std
            action = torch.clamp(action + noise, -1.0, 1.0)
        return action, None, value

    def evaluate(
        self,
        x: Tensor,
        actions: Optional[Tensor] = None,
        noise_std: float = 0.0,
    ) -> Tuple[Optional[Tensor], Optional[Tensor], Tensor, Tensor]:
        if actions is None:
            actions, value = self.forward(x)
        else:
            value = self.critic(torch.cat([x, actions], dim=-1)).squeeze(-1)
        if noise_std > 0.0:
            actions = torch.clamp(actions + torch.randn_like(actions) * noise_std, -1.0, 1.0)
        return None, None, value, actions


class _PositionalEncoding(nn.Module):
    def __init__(self, d_model: int, max_len: int = 5000) -> None:
        super().__init__()
        position = torch.arange(0, max_len).unsqueeze(1)
        div_term = torch.exp(torch.arange(0, d_model, 2) * (-torch.log(torch.tensor(10000.0)) / d_model))
        pe = torch.zeros(max_len, d_model)
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        self.register_buffer("pe", pe.unsqueeze(0), persistent=False)

    def forward(self, x: Tensor) -> Tensor:
        return x + self.pe[:, : x.size(1)]


class TransformerPolicy(nn.Module):
    """Transformer-based policy that exposes logits and value estimates."""

    def __init__(
        self,
        input_size: int = 8,
        hidden_size: int = 32,
        num_layers: int = 2,
        num_heads: int = 4,
        clip_epsilon: float = 0.2,
    ) -> None:
        super().__init__()
        self.clip_epsilon = clip_epsilon
        self.embed = nn.Linear(input_size, hidden_size)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_size,
            nhead=num_heads,
            dim_feedforward=hidden_size * 2,
            activation="relu",
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers)
        self.positional_encoding = _PositionalEncoding(hidden_size)
        self.policy_head = nn.Linear(hidden_size, 2)
        self.value_head = nn.Linear(hidden_size, 1)
        self.apply(_orthogonal_init)

    def _flatten_input(self, x: Tensor) -> Tensor:
        if x.dim() == 2:
            return x.unsqueeze(1)
        if x.dim() != 3:
            raise ValueError("TransformerPolicy expects input with shape [B, F] or [B, T, F]")
        return x

    def forward(self, x: Tensor) -> Tuple[Tensor, Tensor]:
        x = self._flatten_input(x)
        embeddings = self.embed(x)
        embeddings = self.positional_encoding(embeddings)
        encoded = self.encoder(embeddings)
        last_hidden = encoded[:, -1]
        logits = self.policy_head(last_hidden)
        value = self.value_head(last_hidden).squeeze(-1)
        return logits, value

    def act(self, x: Tensor, deterministic: bool = False) -> Tuple[Tensor, Tensor, Tensor]:
        logits, value = self.forward(x)
        dist = Categorical(logits=logits)
        if deterministic:
            action = torch.argmax(logits, dim=-1)
        else:
            action = dist.sample()
        log_prob = dist.log_prob(action)
        return action, log_prob, value

    def evaluate(
        self, x: Tensor, actions: Tensor
    ) -> Tuple[Tensor, Tensor, Tensor, Optional[Tensor]]:
        logits, value = self.forward(x)
        dist = Categorical(logits=logits)
        log_prob = dist.log_prob(actions)
        entropy = dist.entropy()
        return log_prob, entropy, value, None


# Backwards compatibility aliases
_A3C = A3CPolicy
_DDPG = DDPGPolicy

__all__ = [
    "A3CPolicy",
    "DDPGPolicy",
    "TransformerPolicy",
    "_A3C",
    "_DDPG",
]
