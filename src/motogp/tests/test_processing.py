import asyncio
import pytest
from motogp.model import TaskQueue, TaskStatus
from motogp.processing import consumer, load_queue, async_load_queue
from motogp.database import setup_duckdb, setup_sqlite


# End-to-end Test
class TestProcessing:
    queue = asyncio.Queue()

    def test_load_queue(cls):
        setup_duckdb(True)
        setup_sqlite(True)

        load_queue(1)

        assert TaskQueue.from_db(TaskStatus.NEW).size == 1

    @pytest.mark.asyncio
    async def test_async_load_queue(cls):
        raise NotImplementedError()
        setup_duckdb(True)
        setup_sqlite(True)

        await async_load_queue(1)

        assert TaskQueue.from_db(TaskStatus.NEW).size == 1

    @pytest.mark.asyncio
    async def test_processing(cls):
        setup_duckdb(True)
        setup_sqlite(True)

        await async_load_queue(1)

        for task in TaskQueue.from_db(TaskStatus.NEW).tasks:
            await cls.queue.put(task)

        consumers = [asyncio.create_task(consumer(cls.queue)) for _ in range(1)]

        await cls.queue.join()

        for c in consumers:
            c.cancel()
