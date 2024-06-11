import asyncio
import pytest
from motogp.model import TaskQueue, TaskStatus
from motogp.consumer import consumer
from motogp.producer import async_produce_tasks, produce_tasks
from motogp.database import setup_duckdb, setup_sqlite


# End-to-end Test
class TestProcessing:
    queue = asyncio.Queue()

    def test_produce_tasks(cls):
        setup_duckdb(True)
        setup_sqlite(True)

        produce_tasks(1)

        assert TaskQueue.from_db(TaskStatus.NEW).size == 1

    @pytest.mark.asyncio
    async def test_async_produce_tasks(cls):
        setup_duckdb(True)
        setup_sqlite(True)

        try:
            await async_produce_tasks(1)
        except asyncio.LimitOverrunError as err:
            pass

        assert TaskQueue.from_db(TaskStatus.NEW).size == 1

    @pytest.mark.asyncio
    async def test_processing(cls):
        setup_duckdb(True)
        setup_sqlite(True)

        produce_tasks(1)

        for task in TaskQueue.from_db(TaskStatus.NEW).tasks:
            await cls.queue.put(task)

        consumers = [asyncio.create_task(consumer(cls.queue)) for _ in range(1)]

        await cls.queue.join()

        for c in consumers:
            c.cancel()
