import asyncio
from collections.abc import Iterable

from solhunter_zero.golden_pipeline.utils import gather_in_batches


def test_gather_in_batches_generator_auto_batch_size() -> None:
    generator = (i for i in range(5))
    calls: list[list[int]] = []

    async def worker(batch: Iterable[int]) -> list[int]:
        calls.append(list(batch))
        await asyncio.sleep(0)
        return [item * 2 for item in batch]

    result = asyncio.run(gather_in_batches(generator, batch_size=0, worker=worker))

    assert result == [0, 2, 4, 6, 8]
    assert calls == [[0, 1, 2, 3, 4]]
