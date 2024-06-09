import asyncio
import os
import uuid
import pytest
from motogp.model import Task, TaskQueue, TaskStatus
from motogp.processing import consumer
from motogp.endpoints import get_categories, get_seasons, get_events, get_sessions
from motogp.database import setup_duckdb, setup_sqlite


class TestProcessing:
    queue = asyncio.Queue()

    @pytest.mark.asyncio
    async def test_processing(cls):
        setup_duckdb(True)
        setup_sqlite(True)
        season = get_seasons()[0]
        event = get_events(season.id)[0]
        category = get_categories(event.id)[0]
        session = get_sessions(event.id, category.id)[0]
        task_id = str(uuid.uuid4())
        task = Task(task_id, season.id, event.id, category.id, session.id)
        task.upsert_status(TaskStatus.NEW)

        for task in TaskQueue.from_db().tasks:
            cls.queue.put_nowait(task)

        consumers = [asyncio.create_task(consumer(cls.queue)) for _ in range(1)]

        await cls.queue.join()

        for c in consumers:
            c.cancel()
