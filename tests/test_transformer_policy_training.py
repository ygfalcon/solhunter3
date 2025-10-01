import pytest
torch = pytest.importorskip("torch")
from solhunter_zero.rl_algorithms import TransformerPolicy

def test_transformer_policy_learns():
    model = TransformerPolicy()
    opt = torch.optim.Adam(model.parameters(), lr=0.01)
    states = torch.randn(5, 8)
    actions = torch.tensor([0,1,0,1,0])
    rewards = torch.randn(5)
    before = [p.clone() for p in model.parameters()]
    for _ in range(3):
        dist = torch.distributions.Categorical(logits=model(states))
        log_probs = dist.log_prob(actions)
        h = model.encoder(model.embed(states).unsqueeze(1))[:, -1]
        values = model.critic(h)
        advantages = rewards - values.squeeze()
        ratio = torch.exp(log_probs - log_probs.detach())
        loss = -(ratio * advantages).mean()
        opt.zero_grad()
        loss.backward()
        opt.step()
    after = list(model.parameters())
    changed = any(not torch.equal(a,b) for a,b in zip(after, before))
    assert changed
