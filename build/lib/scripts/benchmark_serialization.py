import argparse
import importlib
import os
import time


def _bench(mode: str, iters: int) -> float:
    os.environ['EVENT_SERIALIZATION'] = mode
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)
    payload = {'a': 1, 'b': [1, 2, 3], 'c': {'d': 4}}
    start = time.perf_counter()
    for _ in range(iters):
        ev._loads(ev._dumps(payload))
    return iters / (time.perf_counter() - start)


def main() -> None:
    parser = argparse.ArgumentParser(description='Benchmark event serialization')
    parser.add_argument('--iterations', type=int, default=10000)
    args = parser.parse_args()
    j_rate = _bench('json', args.iterations)
    m_rate = _bench('msgpack', args.iterations)
    print(f'json:    {j_rate:.0f} ops/s')
    print(f'msgpack: {m_rate:.0f} ops/s')


if __name__ == '__main__':
    main()
