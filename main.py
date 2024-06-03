# MOTOGP Results Scraper
import asyncio
import logging
import sys
from typing import List

import httpx

from database import setup_db
from model import Record
from requests import exceptions
from endpoints import (
    get_categories,
    get_events,
    get_classification,
    get_seasons,
    get_sessions,
)

file_handler = logging.FileHandler(filename="scrape.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers = [file_handler, stdout_handler]

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
    handlers=handlers,
)


# scrape
async def consumer(client, queue, conn):
    while True:
        logger = logging.getLogger("scraper")
        season, event, category, session = await queue.get()

        if not session.missing(conn):
            logger.info(
                "already have classification associated with %s/%s/%s/%s",
                season.year,
                event.short_name,
                category.name,
                session.name,
            )
            return None

        logger.info(
            "requesting classification for %s/%s/%s/%s",
            season.year,
            event.short_name,
            category.name,
            session.name,
        )

        try:
            classification = await get_classification(client, session.id)
        except exceptions.HTTPError as err:
            classification = None
            logger.error(f"http error: {err}")

        if not classification:
            logger.error(
                "failed to get classification for %s/%s/%s/%s",
                season.year,
                event.short_name,
                category.name,
                session.name,
            )
            return None

        session.update_status(conn)
        record = Record(season, event, category, session, classification)
        record.write(conn)
        queue.task_done()


def tasks() -> List:
    tasks = []
    for season in get_seasons():
        for event in get_events(season.id):
            for category in get_categories(event.id):
                for session in get_sessions(event.id, category.id):
                    tasks.append([season, event, category, session])

    return tasks


async def main():
    logger = logging.getLogger("scraper")
    logger.info("started")
    conn = setup_db(fresh=True)
    client = httpx.AsyncClient()

    queue = asyncio.Queue()

    for task in tasks():
        await queue.put(task)

    consumers = [asyncio.create_task(consumer(client, queue, conn)) for _ in range(10)]

    await queue.join()

    for c in consumers:
        c.cancel()

    # export
    conn.execute(
        """\
COPY (
    SELECT
        season_year,
        event_short_name,
        category_name,
        session_name,
        rider_name,
        rider_position,
        rider_points
    FROM records
) TO 'output.csv'"""
    )

    logger.info("finished")


asyncio.run(main())
