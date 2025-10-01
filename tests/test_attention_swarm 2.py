import pytest
torch = pytest.importorskip("torch.nn.utils.rnn")
from solhunter_zero.agents.attention_swarm import AttentionSwarm, save_model, load_model


def test_load_model_device_cuda(tmp_path, monkeypatch):
    model = AttentionSwarm(2)
    path = tmp_path / "m.pt"
    save_model(model, path)

    monkeypatch.setattr(torch.cuda, "is_available", lambda: True)

    orig_module_to = torch.nn.Module.to

    def fake_to(self, device, *a, **k):
        self._dev = torch.device(device)
        return self

    monkeypatch.setattr(torch.nn.Module, "to", fake_to)
    orig_load = torch.load
    monkeypatch.setattr(torch, "load", lambda p, map_location=None: orig_load(p, map_location="cpu"))

    loaded = load_model(str(path), device="cuda")
    assert loaded.device.type == "cuda"
    assert getattr(loaded, "_dev").type == "cuda"

