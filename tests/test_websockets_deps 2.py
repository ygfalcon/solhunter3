def test_websockets_required(monkeypatch):
    """check_deps should report websockets as required when missing"""
    import sys, pkgutil

    # Avoid heavy config import in solhunter_zero.device
    monkeypatch.setenv("TORCH_METAL_VERSION", "0")
    monkeypatch.setenv("TORCHVISION_METAL_VERSION", "0")
    from scripts import deps

    # Remove stubbed websockets modules
    monkeypatch.delitem(sys.modules, "websockets", raising=False)
    monkeypatch.delitem(sys.modules, "websockets.legacy", raising=False)
    monkeypatch.delitem(sys.modules, "websockets.legacy.client", raising=False)

    real_find_loader = pkgutil.find_loader

    def fake_find_loader(name):
        if name == "websockets":
            return None
        return real_find_loader(name)

    monkeypatch.setattr(pkgutil, "find_loader", fake_find_loader)

    missing, _ = deps.check_deps()
    assert "websockets" in missing
