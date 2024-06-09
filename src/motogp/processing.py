import asyncio
import logging
import sys
import uuid

import httpx

from motogp.database import setup_duckdb
from motogp.model import Task, TaskQueue, TaskStatus
from motogp.endpoints import (
    get_classification,
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
            classification = await get_classification(task)
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


def load_queue(incremental: bool = True):
    logger = setup_logger()
    for season in get_seasons(incremental):
        logger.info("enqueuing season: %s", season.year)
        for event in get_events(season.id, incremental):
            logger.info("enqueuing event: %s", event.short_name)
            for category in get_categories(event.id):
                logger.info("enqueuing category: %s", category.name)
                for session in get_sessions(event.id, category.id):
                    logger.info("enqueuing session: %s", session.name)
                    season.sync()
                    event.sync()
                    category.sync()
                    session.sync()
                    task = Task(uuid.uuid4(), season, event, category, session)
                    task.upsert_status(TaskStatus.NEW)


def export_results():
    conn = setup_duckdb()
    conn.execute("copy dwh.vw_results to 'motogp.parquet' (FORMAT PARQUET)")
    conn.close()
