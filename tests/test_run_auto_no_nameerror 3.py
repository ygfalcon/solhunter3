import sys
import types
import contextlib
import importlib.machinery

import solhunter_zero.config as cfg_mod


def test_run_auto_no_nameerror(monkeypatch, tmp_path):
    with monkeypatch.context() as mp:
        faiss_mod = types.ModuleType("faiss")
        faiss_mod.__spec__ = importlib.machinery.ModuleSpec("faiss", loader=None)
        mp.setitem(sys.modules, "faiss", faiss_mod)

        st_mod = types.ModuleType("sentence_transformers")
        st_mod.__spec__ = importlib.machinery.ModuleSpec("sentence_transformers", loader=None)
        st_mod.SentenceTransformer = lambda *a, **k: types.SimpleNamespace(
            get_sentence_embedding_dimension=lambda: 1, encode=lambda x: []
        )
        mp.setitem(sys.modules, "sentence_transformers", st_mod)

        sklearn = types.ModuleType("sklearn")
        sklearn.__spec__ = importlib.machinery.ModuleSpec("sklearn", loader=None)
        mp.setitem(sys.modules, "sklearn", sklearn)
        mp.setitem(
            sys.modules,
            "sklearn.linear_model",
            types.SimpleNamespace(LinearRegression=object),
        )
        mp.setitem(
            sys.modules,
            "sklearn.ensemble",
            types.SimpleNamespace(GradientBoostingRegressor=object, RandomForestRegressor=object),
        )
        mp.setitem(
            sys.modules,
            "sklearn.cluster",
            types.SimpleNamespace(KMeans=object, DBSCAN=object),
        )
        xgb_mod = types.ModuleType("xgboost")
        xgb_mod.__spec__ = importlib.machinery.ModuleSpec("xgboost", loader=None)
        xgb_mod.XGBRegressor = object
        mp.setitem(sys.modules, "xgboost", xgb_mod)

        torch_mod = types.ModuleType("torch")
        torch_mod.__spec__ = importlib.machinery.ModuleSpec("torch", loader=None)
        torch_mod.no_grad = contextlib.nullcontext
        torch_mod.tensor = lambda *a, **k: None
        torch_mod.nn = types.SimpleNamespace(
            LSTM=object,
            Linear=object,
            TransformerEncoder=object,
            TransformerEncoderLayer=object,
            Module=object,
        )
        torch_mod.optim = types.ModuleType("optim")
        torch_mod.optim.__spec__ = importlib.machinery.ModuleSpec("torch.optim", loader=None)
        torch_mod.utils = types.ModuleType("utils")
        torch_mod.utils.data = types.SimpleNamespace(Dataset=object, DataLoader=object)
        mp.setitem(sys.modules, "torch", torch_mod)
        mp.setitem(sys.modules, "torch.utils", torch_mod.utils)
        mp.setitem(sys.modules, "torch.utils.data", torch_mod.utils.data)
        mp.setitem(sys.modules, "torch.nn", torch_mod.nn)

        pl_mod = types.ModuleType("pytorch_lightning")
        pl_mod.callbacks = types.SimpleNamespace(Callback=object)
        pl_mod.LightningModule = object
        pl_mod.LightningDataModule = object
        pl_mod.Trainer = object
        mp.setitem(sys.modules, "pytorch_lightning", pl_mod)

        stub_models = types.ModuleType("solhunter_zero.models")
        stub_models.get_model = lambda *a, **k: None
        stub_models.load_compiled_model = lambda *a, **k: None
        stub_models.export_torchscript = lambda *a, **k: None
        mp.setitem(sys.modules, "solhunter_zero.models", stub_models)
        mp.setitem(
            sys.modules,
            "solhunter_zero.models.regime_classifier",
            types.SimpleNamespace(get_model=lambda *a, **k: None),
        )
        stub_models.regime_classifier = sys.modules[
            "solhunter_zero.models.regime_classifier"
        ]

        from solhunter_zero import main as main_module
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        cfg_file = cfg_dir / "cfg.toml"
        cfg_file.write_text("risk_tolerance=0.5")
        (cfg_dir / "active").write_text("cfg.toml")
        monkeypatch.setattr(cfg_mod, "CONFIG_DIR", str(cfg_dir))
        monkeypatch.setattr(cfg_mod, "ACTIVE_CONFIG_FILE", str(cfg_dir / "active"))
        monkeypatch.setattr(main_module, "CONFIG_DIR", str(cfg_dir))

        monkeypatch.setattr(main_module.wallet, "get_active_keypair_name", lambda: "kp")
        monkeypatch.setattr(main_module, "_start_depth_service", lambda cfg: None)
        called = {}
        monkeypatch.setattr(main_module, "main", lambda **kw: called.setdefault("called", True))
        sys.modules["solhunter_zero.data_sync"] = types.SimpleNamespace(sync_recent=lambda: None)
        main_module.data_sync = sys.modules["solhunter_zero.data_sync"]

        main_module.run_auto(iterations=1, dry_run=True)
        assert called.get("called") is True
