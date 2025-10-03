# Helper module providing lightweight stubs for optional dependencies.

from __future__ import annotations

import sys
import types
import time
import importlib
import importlib.machinery
import os


def stub_numpy() -> None:
    if 'numpy' in sys.modules:
        return
    try:
        spec = importlib.util.find_spec('numpy')
    except Exception:  # pragma: no cover - safety net
        spec = None
    if spec is not None:
        mod = importlib.import_module('numpy')
        sys.modules.setdefault('numpy', mod)
        return

    np = types.ModuleType('numpy')
    np.__spec__ = importlib.machinery.ModuleSpec('numpy', None)
    np._STUB = True

    def _make(shape, value):
        if isinstance(shape, int):
            return [value for _ in range(shape)]
        if shape == ():
            return value
        first, *rest = shape
        return [_make(tuple(rest), value) for _ in range(first)]

    def array(obj, dtype=None):
        return list(obj)

    def asarray(obj, dtype=None):
        return list(obj)

    def zeros(shape, dtype=float):
        return _make(shape, 0)

    def ones(shape, dtype=float):
        return _make(shape, 1)

    np.array = array
    np.asarray = asarray
    np.zeros = zeros
    np.ones = ones
    np.bool_ = bool
    np.isscalar = lambda x: not isinstance(x, (list, tuple, dict))
    sys.modules.setdefault('numpy', np)


def stub_cachetools() -> None:
    if 'cachetools' in sys.modules:
        return
    ct = types.ModuleType('cachetools')
    ct.__spec__ = importlib.machinery.ModuleSpec('cachetools', None)

    class LRUCache(dict):
        def __init__(self, maxsize: int = 128, *a, **k) -> None:
            self.maxsize = maxsize
            super().__init__(*a, **k)

        def __setitem__(self, key, value) -> None:
            if key in self:
                super().__delitem__(key)
            elif len(self) >= self.maxsize:
                oldest = next(iter(self))
                super().__delitem__(oldest)
            super().__setitem__(key, value)

    class TTLCache(dict):
        def __init__(self, maxsize: int = 128, ttl: float = 60.0) -> None:
            self.maxsize = maxsize
            self.ttl = ttl
            self._exp = {}

        def __setitem__(self, key, value) -> None:
            now = time.monotonic()
            if key in self:
                self._exp[key] = now + self.ttl
                dict.__setitem__(self, key, value)
                return
            while len(self) >= self.maxsize:
                k = next(iter(self))
                if self._exp.get(k, 0) <= now:
                    dict.pop(self, k, None)
                    self._exp.pop(k, None)
                else:
                    break
            self._exp[key] = now + self.ttl
            dict.__setitem__(self, key, value)

        def __getitem__(self, key):
            if key not in self or self._exp.get(key, 0) <= time.monotonic():
                raise KeyError(key)
            return dict.__getitem__(self, key)

        def get(self, key, default=None):
            try:
                return self.__getitem__(key)
            except KeyError:
                return default

        def pop(self, key, default=None):
            self._exp.pop(key, None)
            return dict.pop(self, key, default)

    ct.LRUCache = LRUCache
    ct.TTLCache = TTLCache
    sys.modules.setdefault('cachetools', ct)


def stub_sqlalchemy() -> None:
    if 'sqlalchemy' in sys.modules:
        return
    sa = types.ModuleType('sqlalchemy')
    sa.__spec__ = importlib.machinery.ModuleSpec('sqlalchemy', None)

    class Column:
        def __init__(self, _type=None, *args, **kwargs):
            self.name = None
            self.default = kwargs.get('default')
            self.primary_key = kwargs.get('primary_key', False)

        def __set_name__(self, owner, name):
            self.name = name

        def __get__(self, obj, objtype=None):
            if obj is None:
                return self
            val = obj.__dict__.get(self.name)
            if val is None:
                val = self.default() if callable(self.default) else self.default
                obj.__dict__[self.name] = val
            return val

        def __set__(self, obj, value):
            obj.__dict__[self.name] = value

        def __eq__(self, other):
            return lambda inst: getattr(inst, self.name) == other

        def __gt__(self, other):
            return lambda inst: getattr(inst, self.name) > other

        def contains(self, item):
            return lambda inst: item in getattr(inst, self.name, '')

        def in_(self, seq):
            return lambda inst: getattr(inst, self.name) in seq

    class MetaData:
        def create_all(self, *a, **k):
            pass

    class Table:
        pass

    _ENGINES: dict[str, Engine] = {}

    def create_engine(url, *a, **k):
        eng = _ENGINES.setdefault(url, Engine())
        return eng

    class Query:
        def __init__(self, model, session=None):
            self.session = session
            self.model = model
            self.filters = []
            self.order = None
            self.limit_n = None

        def with_session(self, session):
            self.session = session
            return self

        def _execute(self):
            if self.session is None:
                raise RuntimeError('unbound query')
            data = list(self.session.engine.storage.get(self.model, []))
            for f in self.filters:
                data = [d for d in data if f(d)]
            if self.order:
                data.sort(key=lambda d: getattr(d, self.order))
            if self.limit_n is not None:
                data = data[:self.limit_n]
            return data

        def __iter__(self):
            return iter(self._execute())

        def all(self):
            return self._execute()

        def first(self):
            data = self._execute()
            return data[0] if data else None

        def filter(self, func):
            self.filters.append(func)
            return self

        def filter_by(self, **kwargs):
            self.filters.append(lambda obj: all(getattr(obj, k) == v for k, v in kwargs.items()))
            return self

        def order_by(self, col):
            self.order = getattr(col, 'name', None)
            return self

        def limit(self, n):
            self.limit_n = n
            return self

    def select(model):
        return Query(model)

    class Insert:
        def __init__(self, model):
            self.model = model

    def insert(model):
        return Insert(model)

    class TextClause:
        def __init__(self, sql: str):
            self.sql = sql

    def text(sql: str):
        return TextClause(sql)

    class Engine:
        def __init__(self):
            self.storage = {}
            self.counters = {}

        def begin(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def run_sync(self, func):
            func(self)

        async def dispose(self):
            pass

    class AsyncSession:
        def __init__(self, engine):
            self.engine = engine

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def add(self, obj):
            lst = self.engine.storage.setdefault(type(obj), [])
            if getattr(obj, 'id', None) is None:
                idx = self.engine.counters.get(type(obj), 1)
                setattr(obj, 'id', idx)
                self.engine.counters[type(obj)] = idx + 1
            lst.append(obj)

        def bulk_save_objects(self, objs):
            for o in objs:
                self.add(o)

        async def commit(self):
            pass

        async def run_sync(self, func):
            func(self)

        async def execute(self, query, params=None):
            if isinstance(query, Query):
                data = query.with_session(self)._execute()
                return Result(data, rowcount=len(data))
            if isinstance(query, Insert):
                objs_params = params if isinstance(params, list) else [params or {}]
                objs = [query.model(**p) for p in objs_params]
                self.bulk_save_objects(objs)
                return Result([], rowcount=len(objs))
            if isinstance(query, TextClause):
                return self._execute_text(query.sql, params or {})
            raise TypeError("unsupported query type")

        def _execute_text(self, sql: str, params: dict):
            import re
            from types import SimpleNamespace
            try:
                from solhunter_zero.memory import Trade
            except Exception:  # pragma: no cover - fallback
                Trade = None
            low = sql.strip().lower()
            if low.startswith("insert into trades") and Trade is not None:
                m = re.search(r"insert\s+into\s+trades\s*\(([^)]*)\)\s*values\s*\(([^)]*)\)", low)
                if m:
                    cols = [c.strip() for c in m.group(1).split(',')]
                    keys = [k.strip().lstrip(':') for k in m.group(2).split(',')]
                    objs = []
                    params_list = params if isinstance(params, list) else [params]
                    for p in params_list:
                        data = {c: p.get(k) for c, k in zip(cols, keys)}
                        objs.append(Trade(**data))
                    self.bulk_save_objects(objs)
                    return Result([], rowcount=len(objs))
            if low.startswith("update trades") and Trade is not None:
                m = re.search(r"update\s+trades\s+set\s+(\w+)\s*=\s*:([a-z_]+)\s+where\s+(\w+)\s*=\s*:([a-z_]+)", low)
                rowcount = 0
                if m:
                    set_col, set_key, cond_col, cond_key = m.groups()
                    for obj in self.engine.storage.get(Trade, []):
                        if getattr(obj, cond_col) == params.get(cond_key):
                            setattr(obj, set_col, params.get(set_key))
                            rowcount += 1
                return Result([], rowcount=rowcount)
            if low.startswith("select") and "from trades" in low and Trade is not None:
                m = re.search(r"select\s+([^\s]+(?:\s*,\s*[^\s]+)*)\s+from\s+trades(?:\s+where\s+(\w+)\s*=\s*:([a-z_]+))?", low)
                if m:
                    cols = [c.strip() for c in m.group(1).split(',')]
                    cond_col = m.group(2)
                    cond_key = m.group(3)
                    rows = []
                    for obj in self.engine.storage.get(Trade, []):
                        if cond_col is not None and getattr(obj, cond_col) != params.get(cond_key):
                            continue
                        row_data = {c: getattr(obj, c) for c in cols}
                        rows.append(SimpleNamespace(_mapping=row_data))
                    return Result(rows, rowcount=len(rows))
            return Result([], rowcount=0)

    class Result:
        def __init__(self, data, rowcount: int = 0):
            self._data = data
            self.rowcount = rowcount

        def scalars(self):
            class _Scalars:
                def __init__(self, data):
                    self._data = data

                def all(self):
                    return self._data

            return _Scalars(self._data)

        def __iter__(self):
            return iter(self._data)

    def sessionmaker(bind=None, expire_on_commit=False):
        def factory(**kw):
            return Session(bind)
        return factory

    def async_sessionmaker(bind=None, expire_on_commit=False):
        def factory(**kw):
            return AsyncSession(bind)
        return factory

    class Session(AsyncSession):
        def query(self, model):
            return Query(model, session=self)

        def commit(self):
            pass

    def declarative_base():
        class Base:
            metadata = MetaData()

            def __init__(self, **kw):
                for k, v in kw.items():
                    setattr(self, k, v)
        return Base

    sa.Column = Column
    sa.Integer = type('Integer', (), {})
    sa.Float = type('Float', (), {})
    sa.String = type('String', (), {})
    sa.DateTime = type('DateTime', (), {})
    sa.Text = type('Text', (), {})
    LargeBinary = object
    sa.LargeBinary = LargeBinary
    sa.ForeignKey = lambda *a, **k: None
    sa.create_engine = create_engine
    sa.MetaData = MetaData
    sa.Table = Table
    sa.select = select
    sa.insert = insert
    sa.text = text
    sa.Insert = Insert
    sa.TextClause = TextClause

    orm = types.ModuleType('sqlalchemy.orm')
    orm.__spec__ = importlib.machinery.ModuleSpec('sqlalchemy.orm', None)
    orm.sessionmaker = sessionmaker
    orm.declarative_base = declarative_base
    sa.orm = orm

    ext = types.ModuleType('sqlalchemy.ext')
    ext.__spec__ = importlib.machinery.ModuleSpec('sqlalchemy.ext', None)
    async_mod = types.ModuleType('sqlalchemy.ext.asyncio')
    async_mod.__spec__ = importlib.machinery.ModuleSpec('sqlalchemy.ext.asyncio', None)

    def create_async_engine(*a, **k):
        return Engine()

    async_mod.create_async_engine = create_async_engine
    async_mod.async_sessionmaker = async_sessionmaker
    async_mod.AsyncSession = AsyncSession
    ext.asyncio = async_mod
    sa.ext = ext

    sys.modules.setdefault('sqlalchemy', sa)
    sys.modules.setdefault('sqlalchemy.orm', orm)
    sys.modules.setdefault('sqlalchemy.ext', ext)
    sys.modules.setdefault('sqlalchemy.ext.asyncio', async_mod)


def stub_watchfiles() -> None:
    if 'watchfiles' in sys.modules:
        return
    mod = types.ModuleType('watchfiles')
    mod.__spec__ = importlib.machinery.ModuleSpec('watchfiles', None)

    async def awatch(*a, **k):
        if False:
            yield None

    mod.awatch = awatch
    sys.modules.setdefault('watchfiles', mod)


def stub_psutil() -> None:
    if 'psutil' in sys.modules:
        return
    mod = types.ModuleType('psutil')
    mod.__spec__ = importlib.machinery.ModuleSpec('psutil', None)
    mod.cpu_percent = lambda *a, **k: 0.0
    mod.virtual_memory = lambda: types.SimpleNamespace(percent=0.0)
    class Process:
        def __init__(self, *a, **k):
            pass

        def cpu_times(self):
            return types.SimpleNamespace(user=0.0)

    mod.Process = Process
    sys.modules.setdefault('psutil', mod)


def stub_flask() -> None:
    if 'flask' in sys.modules:
        return
    import importlib.util
    if importlib.util.find_spec('flask') is not None:
        return
    flask = types.ModuleType('flask')
    flask.__spec__ = importlib.machinery.ModuleSpec('flask', None)

    request = types.SimpleNamespace(json=None, files={})

    class Flask:
        def __init__(self, name, static_folder=None):
            self.routes = {}

        def route(self, path, methods=None):
            if methods is None:
                methods = ['GET']
            methods = tuple(m.upper() for m in methods)

            def decorator(func):
                self.routes[(path, methods)] = func
                return func

            return decorator

        def test_client(self):
            app = self

            class Client:
                def open(self, path, method='GET', json=None):
                    request.json = json
                    for (p, m), func in app.routes.items():
                        if p == path and method.upper() in m:
                            data = func()
                            if isinstance(data, tuple):
                                return Response(data[0], data[1])
                            return Response(data)
                    return Response(None)

                def get(self, path):
                    return self.open(path, 'GET')

                def post(self, path, json=None):
                    return self.open(path, 'POST', json=json)

            return Client()

        def register_blueprint(self, bp):
            self.routes.update(getattr(bp, "routes", {}))

    class Response:
        def __init__(self, data, status=200):
            self._data = data
            self.status_code = status

        def get_json(self):
            return self._data

    def jsonify(obj=None):
        return obj if obj is not None else {}

    def render_template_string(tpl):
        return tpl

    flask.Flask = Flask
    flask.Blueprint = Flask
    flask.jsonify = jsonify
    flask.request = request
    flask.render_template_string = render_template_string
    sys.modules.setdefault('flask', flask)


def stub_requests() -> None:
    if 'requests' in sys.modules:
        return
    mod = types.ModuleType('requests')
    mod.__spec__ = importlib.machinery.ModuleSpec('requests', None)

    class HTTPError(Exception):
        pass

    exceptions = types.ModuleType('requests.exceptions')
    exceptions.__spec__ = importlib.machinery.ModuleSpec('requests.exceptions', None)
    exceptions.HTTPError = HTTPError

    mod.HTTPError = HTTPError
    mod.exceptions = exceptions
    mod.get = lambda *a, **k: None
    mod.post = lambda *a, **k: None
    sys.modules.setdefault('requests', mod)
    sys.modules.setdefault('requests.exceptions', exceptions)


def stub_websockets() -> None:
    if 'websockets' in sys.modules:
        return
    mod = types.ModuleType('websockets')
    mod.__spec__ = importlib.machinery.ModuleSpec('websockets', None)

    class DummyConn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def send(self, msg):
            pass

        async def recv(self):
            await asyncio.sleep(0)
            raise StopAsyncIteration

    async def connect(*a, **k):
        return DummyConn()


    async def serve(handler, host, port):
        class Server:
            sockets = [types.SimpleNamespace(getsockname=lambda: (host, port))]

            def close(self):
                pass

            async def wait_closed(self):
                pass

        return Server()

    import asyncio

    mod.connect = connect
    mod.serve = serve

    # create websockets.legacy.client submodules for compatibility
    legacy = types.ModuleType('websockets.legacy')
    legacy.__spec__ = importlib.machinery.ModuleSpec('websockets.legacy', None)
    client = types.ModuleType('websockets.legacy.client')
    client.__spec__ = importlib.machinery.ModuleSpec('websockets.legacy.client', None)

    class WebSocketClientProtocol(DummyConn):
        pass

    client.WebSocketClientProtocol = WebSocketClientProtocol
    client.connect = connect
    legacy.client = client
    mod.legacy = legacy

    sys.modules.setdefault('websockets', mod)
    sys.modules.setdefault('websockets.legacy', legacy)
    sys.modules.setdefault('websockets.legacy.client', client)


def stub_aiofiles() -> None:
    if 'aiofiles' in sys.modules:
        return
    mod = types.ModuleType('aiofiles')
    mod.__spec__ = importlib.machinery.ModuleSpec('aiofiles', None)

    class _DummyFile:
        def __init__(self):
            self._data = ''

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def read(self):
            return self._data

        async def write(self, data):
            self._data += str(data)
            return len(data)

    def open(*a, **k):
        return _DummyFile()

    mod.open = open
    sys.modules.setdefault('aiofiles', mod)


def stub_bip_utils() -> None:
    if 'bip_utils' in sys.modules:
        return
    mod = types.ModuleType('bip_utils')
    mod.__spec__ = importlib.machinery.ModuleSpec('bip_utils', None)

    class Bip39SeedGenerator:
        def __init__(self, mnemonic: str):
            self.mnemonic = mnemonic

        def Generate(self, passphrase: str = ''):
            return b'\0' * 32

    class _Deriver:
        def Purpose(self):
            return self

        def Coin(self):
            return self

        def Account(self, _):
            return self

        def Change(self, _):
            return self

        def AddressIndex(self, _):
            return self

        def PrivateKey(self):
            return self

        def Raw(self):
            return self

        def ToBytes(self):
            return b'\0' * 32

    class Bip44:
        @staticmethod
        def FromSeed(seed, coin):
            return _Deriver()

    class Bip44Coins:
        SOLANA = object()

    class Bip44Changes:
        CHAIN_EXT = object()

    mod.Bip39SeedGenerator = Bip39SeedGenerator
    mod.Bip44 = Bip44
    mod.Bip44Coins = Bip44Coins
    mod.Bip44Changes = Bip44Changes
    sys.modules.setdefault('bip_utils', mod)


def stub_faiss() -> None:
    if 'faiss' in sys.modules:
        return
    try:
        spec = importlib.util.find_spec('faiss')
    except Exception:  # pragma: no cover - safety net
        spec = None
    if spec is not None:
        mod = importlib.import_module('faiss')
        sys.modules.setdefault('faiss', mod)
        return
    mod = types.ModuleType('faiss')
    mod.__spec__ = importlib.machinery.ModuleSpec('faiss', None)
    mod._STUB = True

    class IndexFlatL2:
        def __init__(self, dim):
            self.dim = dim

    class IndexIDMap2:
        def __init__(self, index):
            self.index = index

    mod.IndexFlatL2 = IndexFlatL2
    mod.IndexIDMap2 = IndexIDMap2
    sys.modules.setdefault('faiss', mod)


def stub_torch() -> None:
    if 'torch' in sys.modules:
        return

    import contextlib

    mod = types.ModuleType('torch')
    mod.__spec__ = importlib.machinery.ModuleSpec('torch', None)
    mod.__path__ = []

    mod.Tensor = object
    mod.float32 = object()
    mod.cuda = types.SimpleNamespace(is_available=lambda: False)
    mod.backends = types.SimpleNamespace(mps=types.SimpleNamespace(is_available=lambda: False))

    mod.no_grad = contextlib.nullcontext

    class Device:
        def __init__(self, name: str = 'cpu') -> None:
            self.type = name

    mod.device = Device

    def tensor(*a, **k):
        return a[0] if a else None

    mod.tensor = tensor
    mod.zeros = lambda *a, **k: [0 for _ in range(a[0])] if a else []
    mod.ones = lambda *a, **k: [1 for _ in range(a[0])] if a else []
    mod.load = lambda *a, **k: {}
    mod.manual_seed = lambda *a, **k: None

    def _save(obj, path, *a, **k):
        with open(path, 'wb') as fh:
            import pickle
            pickle.dump(obj, fh)

    mod.save = _save

    nn = types.ModuleType('torch.nn')
    nn.__spec__ = importlib.machinery.ModuleSpec('torch.nn', None)
    nn.__path__ = []

    class Module:
        def __init__(self, *a, **k):
            pass

        def to(self, *a, **k):
            return self

        def load_state_dict(self, *a, **k):
            pass

        def state_dict(self):
            return {}

        def eval(self):
            return self

    nn.Module = Module
    nn.Sequential = lambda *a, **k: Module()
    nn.Linear = lambda *a, **k: Module()
    nn.ReLU = lambda *a, **k: Module()
    nn.MSELoss = lambda *a, **k: Module()
    nn.Softmax = lambda *a, **k: Module()
    nn.TransformerEncoderLayer = lambda *a, **k: Module()
    nn.TransformerEncoder = lambda *a, **k: Module()
    class Parameter(Module):
        def __init__(self, data=None):
            super().__init__()
            self.data = data

        def detach(self):
            return self

        def cpu(self):
            return self

        def tolist(self):
            return list(self.data) if isinstance(self.data, list) else []

    nn.Parameter = Parameter

    functional = types.ModuleType('torch.nn.functional')
    functional.__spec__ = importlib.machinery.ModuleSpec('torch.nn.functional', None)
    functional.relu = lambda x, inplace=False: x
    functional.softmax = lambda input, dim=None: input
    functional.mse_loss = lambda input, target, *a, **k: 0.0
    nn.functional = functional

    optim = types.ModuleType('torch.optim')
    optim.__spec__ = importlib.machinery.ModuleSpec('torch.optim', None)
    optim.__path__ = []
    optim.Adam = lambda *a, **k: object()

    utils = types.ModuleType('torch.utils')
    utils.__spec__ = importlib.machinery.ModuleSpec('torch.utils', None)
    utils.__path__ = []

    data = types.ModuleType('torch.utils.data')
    data.__spec__ = importlib.machinery.ModuleSpec('torch.utils.data', None)

    class Dataset:
        pass

    class DataLoader:
        def __init__(self, dataset, *a, **k):
            self.dataset = dataset

    data.Dataset = Dataset
    data.DataLoader = DataLoader
    utils.data = data

    nn_utils = types.ModuleType('torch.nn.utils')
    nn_utils.__spec__ = importlib.machinery.ModuleSpec('torch.nn.utils', None)
    rnn_mod = types.ModuleType('torch.nn.utils.rnn')
    rnn_mod.__spec__ = importlib.machinery.ModuleSpec('torch.nn.utils.rnn', None)
    rnn_mod.pad_sequence = lambda *a, **k: []
    nn_utils.rnn = rnn_mod

    mod.nn = nn
    mod.optim = optim
    mod.utils = utils

    sys.modules.setdefault('torch.nn.utils', nn_utils)
    sys.modules.setdefault('torch.nn.utils.rnn', rnn_mod)

    sys.modules.setdefault('torch', mod)
    sys.modules.setdefault('torch.nn', nn)
    sys.modules.setdefault('torch.nn.functional', functional)
    sys.modules.setdefault('torch.optim', optim)
    sys.modules.setdefault('torch.utils', utils)
    sys.modules.setdefault('torch.utils.data', data)


def stub_sklearn() -> None:
    if 'sklearn' in sys.modules:
        return

    sk = types.ModuleType('sklearn')
    sk.__spec__ = importlib.machinery.ModuleSpec('sklearn', None)

    lm = types.ModuleType('sklearn.linear_model')
    lm.__spec__ = importlib.machinery.ModuleSpec('sklearn.linear_model', None)

    class LinearRegression:
        def fit(self, *a, **k):
            return self

        def predict(self, X):
            return [0 for _ in range(len(X))]

    lm.LinearRegression = LinearRegression

    ensemble = types.ModuleType('sklearn.ensemble')
    ensemble.__spec__ = importlib.machinery.ModuleSpec('sklearn.ensemble', None)

    class GradientBoostingRegressor:
        def fit(self, *a, **k):
            return self

        def predict(self, X):
            return [0 for _ in range(len(X))]

    class RandomForestRegressor:
        def __init__(self, *a, **k):
            pass

        def fit(self, *a, **k):
            return self

        def predict(self, X):
            return [0 for _ in range(len(X))]

    ensemble.GradientBoostingRegressor = GradientBoostingRegressor
    ensemble.RandomForestRegressor = RandomForestRegressor

    cluster = types.ModuleType('sklearn.cluster')
    cluster.__spec__ = importlib.machinery.ModuleSpec('sklearn.cluster', None)

    class KMeans:
        def __init__(self, *a, **k):
            pass

        def fit(self, *a, **k):
            return self

        def predict(self, X):
            return [0 for _ in range(len(X))]

    class DBSCAN:
        def fit(self, *a, **k):
            return self

    cluster.KMeans = KMeans
    cluster.DBSCAN = DBSCAN

    gp = types.ModuleType('sklearn.gaussian_process')
    gp.__spec__ = importlib.machinery.ModuleSpec('sklearn.gaussian_process', None)

    class GaussianProcessRegressor:
        def __init__(self, kernel=None, *a, **k):
            self.kernel = kernel

        def fit(self, *a, **k):
            return self

        def predict(self, X):
            return [0 for _ in range(len(X))]

    gp.GaussianProcessRegressor = GaussianProcessRegressor

    kernels = types.ModuleType('sklearn.gaussian_process.kernels')
    kernels.__spec__ = importlib.machinery.ModuleSpec('sklearn.gaussian_process.kernels', None)

    class RBF:
        def __init__(self, *a, **k):
            pass

    class ConstantKernel:
        def __init__(self, *a, **k):
            pass

    class Matern:
        def __init__(self, *a, **k):
            pass

    kernels.RBF = RBF
    kernels.ConstantKernel = ConstantKernel
    kernels.C = ConstantKernel
    kernels.Matern = Matern

    sk.linear_model = lm
    sk.ensemble = ensemble
    sk.cluster = cluster
    sk.gaussian_process = gp
    gp.kernels = kernels

    sys.modules.setdefault('sklearn', sk)
    sys.modules.setdefault('sklearn.linear_model', lm)
    sys.modules.setdefault('sklearn.ensemble', ensemble)
    sys.modules.setdefault('sklearn.cluster', cluster)
    sys.modules.setdefault('sklearn.gaussian_process', gp)
    sys.modules.setdefault('sklearn.gaussian_process.kernels', kernels)


def stub_transformers() -> None:
    """Provide lightweight stubs for the ``transformers`` package."""
    try:
        import importlib.util

        spec = importlib.util.find_spec('transformers')
        if spec is not None and spec.loader is not None:
            return
    except Exception:  # pragma: no cover - safety net
        pass

    mod = sys.modules.get('transformers')
    if mod is None:
        mod = types.ModuleType('transformers')
        mod.__spec__ = importlib.machinery.ModuleSpec('transformers', None)
        sys.modules['transformers'] = mod

    class AutoTokenizer:
        @classmethod
        def from_pretrained(cls, *a, **k):
            return cls()

    class AutoModelForCausalLM:
        @classmethod
        def from_pretrained(cls, *a, **k):
            return cls()

    if not hasattr(mod, 'AutoTokenizer'):
        mod.AutoTokenizer = AutoTokenizer
    if not hasattr(mod, 'AutoModelForCausalLM'):
        mod.AutoModelForCausalLM = AutoModelForCausalLM
    if not hasattr(mod, 'pipeline'):
        mod.pipeline = lambda *a, **k: lambda *aa, **kw: None


def install_stubs() -> None:
    stub_numpy()
    stub_cachetools()
    stub_sqlalchemy()
    stub_watchfiles()
    stub_psutil()
    stub_flask()
    stub_requests()
    stub_aiofiles()
    stub_websockets()
    stub_torch()
    stub_sklearn()
    stub_transformers()
    stub_bip_utils()
    stub_faiss()

    if os.getenv("SOLHUNTER_PATCH_INVESTOR_DEMO"):
        from solhunter_zero.simple_memory import SimpleMemory
        import solhunter_zero.investor_demo as demo

        class _Mem(SimpleMemory):
            def __init__(self, *a, **k):
                super().__init__()

        demo.Memory = _Mem

        async def _fake_arbitrage() -> dict:
            demo.used_trade_types.add("arbitrage")
            return {"path": ["dex1", "dex2"], "profit": 1.0}

        async def _fake_flash_loan() -> str:
            demo.used_trade_types.add("flash_loan")
            return "sig"

        async def _fake_sniper() -> list[str]:
            demo.used_trade_types.add("sniper")
            return ["TKN"]

        async def _fake_dex() -> list[str]:
            demo.used_trade_types.add("dex_scanner")
            return ["pool1"]

        async def _fake_route() -> dict:
            demo.used_trade_types.add("route_ffi")
            return {"path": ["r1", "r2"], "profit": 0.5}

        async def _fake_jito() -> int:
            demo.used_trade_types.add("jito_stream")
            return 0

        demo._demo_arbitrage = _fake_arbitrage
        demo._demo_flash_loan = _fake_flash_loan
        demo._demo_sniper = _fake_sniper
        demo._demo_dex_scanner = _fake_dex
        demo._demo_route_ffi = _fake_route
        demo._demo_jito_stream = _fake_jito


install_stubs()
