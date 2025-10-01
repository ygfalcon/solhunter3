from .jsonutil import dumps, loads
import os
import ctypes
import struct
import logging
import time
import platform

from .system import set_rayon_threads

# Ensure Rayon thread count is configured for FFI
set_rayon_threads()

logger = logging.getLogger(__name__)

LIB = None

_libname = os.environ.get("ROUTE_FFI_LIB")
if _libname is None:
    libfile = (
        "libroute_ffi.dylib" if platform.system() == "Darwin" else "libroute_ffi.so"
    )
    _libname = os.path.join(os.path.dirname(__file__), libfile)
if os.path.exists(_libname):
    try:
        LIB = ctypes.CDLL(_libname)
        _search = getattr(LIB, "search_route_json", None)
        LIB.best_route_json.argtypes = [
            ctypes.c_char_p,  # prices
            ctypes.c_char_p,  # fees
            ctypes.c_char_p,  # gas
            ctypes.c_char_p,  # latency
            ctypes.c_double,
            ctypes.c_uint,
            ctypes.POINTER(ctypes.c_double),
        ]
        LIB.best_route_json.restype = ctypes.c_void_p
        if _search is not None:
            _search.argtypes = list(LIB.best_route_json.argtypes)
            _search.restype = ctypes.c_void_p
            LIB.search_route_json = _search
        _parallel = getattr(LIB, "best_route_parallel_json", None)
        if _parallel is not None:
            _parallel.argtypes = list(LIB.best_route_json.argtypes)
            _parallel.restype = ctypes.c_void_p
            LIB.best_route_parallel_json = _parallel
        _bin = getattr(LIB, "best_route_bin", None)
        if _bin is not None:
            _bin.argtypes = [
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_double,
                ctypes.c_uint,
                ctypes.POINTER(ctypes.c_double),
                ctypes.POINTER(ctypes.c_size_t),
            ]
            _bin.restype = ctypes.c_void_p
            LIB.best_route_bin = _bin
        _bin_search = getattr(LIB, "search_route_bin", None)
        if _bin_search is not None:
            _bin_search.argtypes = list(LIB.best_route_bin.argtypes)
            _bin_search.restype = ctypes.c_void_p
            LIB.search_route_bin = _bin_search
        _bin_par = getattr(LIB, "best_route_parallel_bin", None)
        if _bin_par is not None:
            _bin_par.argtypes = list(LIB.best_route_bin.argtypes)
            _bin_par.restype = ctypes.c_void_p
            LIB.best_route_parallel_bin = _bin_par
        _parallel_flag = getattr(LIB, "route_parallel_enabled", None)
        if _parallel_flag is not None:
            _parallel_flag.restype = ctypes.c_bool
            LIB.route_parallel_enabled = _parallel_flag
        _decode = getattr(LIB, "decode_token_agg_json", None)
        if _decode is not None:
            _decode.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            _decode.restype = ctypes.c_void_p
            LIB.decode_token_agg_json = _decode
        _measure = getattr(LIB, "measure_latency_json", None)
        if _measure is not None:
            _measure.argtypes = [ctypes.c_char_p, ctypes.c_uint]
            _measure.restype = ctypes.c_void_p
            LIB.measure_latency_json = _measure
        LIB.free_cstring.argtypes = [ctypes.c_void_p]
        _free_buf = getattr(LIB, "free_buffer", None)
        if _free_buf is not None:
            _free_buf.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
            _free_buf.restype = None
            LIB.free_buffer = _free_buf
    except OSError as exc:
        logger.warning("Failed to load route FFI library at %s: %s", _libname, exc)
        LIB = None


def _encode_map_bin(data: dict[str, float]) -> bytes:
    parts = [struct.pack("<Q", len(data))]
    for k, v in data.items():
        b = k.encode()
        parts.append(struct.pack("<Q", len(b)))
        parts.append(b)
        parts.append(struct.pack("<d", float(v)))
    return b"".join(parts)


def _decode_vec_str(buf: bytes) -> list[str]:
    off = 0
    if len(buf) < 8:
        return []
    count = struct.unpack_from("<Q", buf, off)[0]
    off += 8
    out = []
    for _ in range(count):
        if off + 8 > len(buf):
            break
        l = struct.unpack_from("<Q", buf, off)[0]
        off += 8
        if off + l > len(buf):
            break
        out.append(buf[off : off + l].decode())
        off += l
    return out


def _best_route_json(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if LIB is None:
        return None
    prof = ctypes.c_double()
    prices_json = dumps(prices).encode()
    fees_json = dumps(fees or {}).encode()
    gas_json = dumps(gas or {}).encode()
    lat_json = dumps(latency or {}).encode()
    func = getattr(LIB, "search_route_json", None)
    if func is None:
        func = LIB.best_route_json
    ptr = func(
        prices_json, fees_json, gas_json, lat_json, amount, max_hops, ctypes.byref(prof)
    )
    if not ptr:
        return None
    path_json = ctypes.string_at(ptr).decode()
    LIB.free_cstring(ptr)
    path = loads(path_json)
    return path, prof.value


def _best_route_parallel_json(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if LIB is None:
        return None
    func = getattr(LIB, "best_route_parallel_json", None)
    if func is None:
        return _best_route_json(
            prices,
            amount,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )
    prof = ctypes.c_double()
    prices_json = dumps(prices).encode()
    fees_json = dumps(fees or {}).encode()
    gas_json = dumps(gas or {}).encode()
    lat_json = dumps(latency or {}).encode()
    ptr = func(
        prices_json,
        fees_json,
        gas_json,
        lat_json,
        amount,
        max_hops,
        ctypes.byref(prof),
    )
    if not ptr:
        return None
    path_json = ctypes.string_at(ptr).decode()
    LIB.free_cstring(ptr)
    path = loads(path_json)
    return path, prof.value


def _best_route_bin(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if LIB is None:
        return None
    func = getattr(LIB, "search_route_bin", None)
    if func is None:
        func = getattr(LIB, "best_route_bin", None)
        if func is None:
            return None
    prof = ctypes.c_double()
    out_len = ctypes.c_size_t()
    pb = _encode_map_bin(prices)
    fb = _encode_map_bin(fees or {})
    gb = _encode_map_bin(gas or {})
    lb = _encode_map_bin(latency or {})
    ptr = func(
        ctypes.c_char_p(pb),
        len(pb),
        ctypes.c_char_p(fb),
        len(fb),
        ctypes.c_char_p(gb),
        len(gb),
        ctypes.c_char_p(lb),
        len(lb),
        amount,
        max_hops,
        ctypes.byref(prof),
        ctypes.byref(out_len),
    )
    if not ptr:
        return None
    buf = ctypes.string_at(ptr, out_len.value)
    if hasattr(LIB, "free_buffer"):
        LIB.free_buffer(ptr, out_len.value)
    path = _decode_vec_str(buf)
    return path, prof.value


def _best_route_parallel_bin(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if LIB is None:
        return None
    func = getattr(LIB, "best_route_parallel_bin", None)
    if func is None:
        return _best_route_bin(
            prices,
            amount,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )
    prof = ctypes.c_double()
    out_len = ctypes.c_size_t()
    pb = _encode_map_bin(prices)
    fb = _encode_map_bin(fees or {})
    gb = _encode_map_bin(gas or {})
    lb = _encode_map_bin(latency or {})
    ptr = func(
        ctypes.c_char_p(pb),
        len(pb),
        ctypes.c_char_p(fb),
        len(fb),
        ctypes.c_char_p(gb),
        len(gb),
        ctypes.c_char_p(lb),
        len(lb),
        amount,
        max_hops,
        ctypes.byref(prof),
        ctypes.byref(out_len),
    )
    if not ptr:
        return None
    buf = ctypes.string_at(ptr, out_len.value)
    if hasattr(LIB, "free_buffer"):
        LIB.free_buffer(ptr, out_len.value)
    path = _decode_vec_str(buf)
    return path, prof.value


_BEST_ROUTE_FUNC = None

if LIB is not None:
    try:
        if hasattr(LIB, "best_route_bin") and hasattr(LIB, "best_route_json"):
            prices = {"dex1": 1.0, "dex2": 1.1}
            fees = {"dex1": 0.0, "dex2": 0.0}
            pb = _encode_map_bin(prices)
            fb = _encode_map_bin(fees)
            json_prices = dumps(prices).encode()
            json_fees = dumps(fees).encode()
            prof = ctypes.c_double()
            out_len = ctypes.c_size_t()
            start = time.perf_counter()
            for _ in range(5):
                ptr = LIB.best_route_json(
                    json_prices,
                    json_fees,
                    json_fees,
                    json_fees,
                    1.0,
                    2,
                    ctypes.byref(prof),
                )
                if ptr:
                    LIB.free_cstring(ptr)
            json_time = time.perf_counter() - start

            start = time.perf_counter()
            for _ in range(5):
                ptr = LIB.best_route_bin(
                    pb,
                    len(pb),
                    fb,
                    len(fb),
                    fb,
                    len(fb),
                    fb,
                    len(fb),
                    1.0,
                    2,
                    ctypes.byref(prof),
                    ctypes.byref(out_len),
                )
                if ptr:
                    LIB.free_buffer(ptr, out_len.value)
            bin_time = time.perf_counter() - start
            _BEST_ROUTE_FUNC = _best_route_bin if bin_time <= json_time else _best_route_json
        else:
            _BEST_ROUTE_FUNC = _best_route_json if hasattr(LIB, "best_route_json") else None
    except Exception:
        _BEST_ROUTE_FUNC = _best_route_json



def available() -> bool:
    return LIB is not None


def is_routeffi_available() -> bool:
    """Return True if the Route FFI library loaded successfully."""
    return available()


def parallel_enabled() -> bool:
    if LIB is None:
        return False
    flag = getattr(LIB, "route_parallel_enabled", None)
    if flag is None:
        return False
    return bool(flag())




def decode_token_agg(buf: bytes) -> dict | None:
    if LIB is None:
        return None
    func = getattr(LIB, "decode_token_agg_json", None)
    if func is None:
        return None
    ptr = func(ctypes.c_char_p(buf), len(buf))
    if not ptr:
        return None
    s = ctypes.string_at(ptr).decode()
    LIB.free_cstring(ptr)
    return loads(s)


def best_route(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if parallel_enabled() and hasattr(LIB, "best_route_parallel_bin"):
        return _best_route_parallel_bin(
            prices,
            amount,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )
    if _BEST_ROUTE_FUNC is None:
        return None
    return _BEST_ROUTE_FUNC(
        prices,
        amount,
        fees=fees,
        gas=gas,
        latency=latency,
        max_hops=max_hops,
    )


def best_route_parallel(
    prices: dict[str, float],
    amount: float,
    *,
    fees=None,
    gas=None,
    latency=None,
    max_hops=4,
):
    if _BEST_ROUTE_FUNC is _best_route_bin and hasattr(LIB, "best_route_parallel_bin"):
        return _best_route_parallel_bin(
            prices,
            amount,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )
    if _BEST_ROUTE_FUNC is _best_route_json and hasattr(LIB, "best_route_parallel_json"):
        return _best_route_parallel_json(
            prices,
            amount,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )
    return best_route(
        prices,
        amount,
        fees=fees,
        gas=gas,
        latency=latency,
        max_hops=max_hops,
    )


def measure_latency(urls: dict[str, str], attempts: int = 3) -> dict | None:
    if LIB is None:
        return None
    func = getattr(LIB, "measure_latency_json", None)
    if func is None:
        return None
    urls_json = dumps(urls).encode()
    ptr = func(urls_json, int(attempts))
    if not ptr:
        return None
    s = ctypes.string_at(ptr).decode()
    LIB.free_cstring(ptr)
    return loads(s)


if LIB is not None and not parallel_enabled():
    logger.error(
        "Parallel route search unavailable. "
        "Rebuild route_ffi with 'cargo build --release --features=parallel'"
    )
