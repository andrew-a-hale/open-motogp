import asyncio
import logging
import sys
import uuid

import httpx

from motogp.database import setup_duckdb
from motogp.model import Task, TaskStatus
from motogp.endpoints import (
    async_get_categories,
    async_get_classification,
    async_get_events,
    async_get_seasons,
    async_get_sessions,
    get_seasons,
    get_events,
    get_categories,
    get_sessions,
)


def setup_logger():
    file_handler = logging.FileHandler(filename="process.log")
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger("processor")


# scrape
async def consumer(queue: asyncio.Queue):
    while True:
        logger = setup_logger()
        task = await queue.get()

        logger.info(
            "requesting classification for session_id: %s",
            task.session_id,
        )

        try:
            classification = await async_get_classification(task)
        except httpx.HTTPStatusError as err:
            classification = None
            logger.error(f"http error: {err}")

        if not classification:
            logger.error(
                "failed to get classification with session_id: %s",
                task.session_id,
            )
            task.upsert_status(TaskStatus.ERROR)
        else:
            classification.sync()
            task.upsert_status(TaskStatus.COMPLETED)

        queue.task_done()


async def async_load_queue(limit: int = 0, incremental: bool = True):
    pass


def load_queue(limit: int = 0, incremental: bool = True):
    count = 0
    logger = setup_logger()

    for season in get_seasons(incremental):
        logger.info("enqueuing season: %s", season.year)
        for event in get_events(season.id, incremental):
            logger.info("enqueuing event: %s", event.short_name)
            for category in get_categories(event.id):
                logger.info("enqueuing category: %s", category.name)
                for session in get_sessions(event.id, category.id):
                    if limit > 0 and count >= limit:
                        logger.info("load_queue hit limit: %s", limit)
                        return None

                    logger.info("enqueuing session: %s", session.name)
                    season.sync()
                    event.sync()
                    category.sync()
                    session.sync()
                    task = Task(
                        str(uuid.uuid4()),
                        season.id,
                        event.id,
                        category.id,
                        session.id,
                    )
                    task.upsert_status(TaskStatus.NEW)
                    count += 1
                    logger.info(
                        "load item: %s/%s", count, limit if limit > 0 else "inf"
                    )


def export_results():
    conn = setup_duckdb()
    conn.execute("copy dwh.vw_results to 'motogp.parquet' (FORMAT PARQUET)")
    conn.close()
