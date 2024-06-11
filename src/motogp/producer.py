# import sys
import asyncio
import sys
import uuid

from motogp.endpoints import (
    async_get_sessions,
    get_seasons,
    get_events,
    get_categories,
    get_sessions,
)
from motogp.logger import setup_logger
from motogp.model import (
    Season,
    Event,
    Category,
    Session,
    Task,
    TaskStatus,
)


async def do_task(
    season: Season,
    event: Event,
    category: Category,
    session: Session,
    count: int,
    limit: int,
):
    logger = setup_logger("producer")

    if limit > 0 and count >= limit:
        logger.info("producer hit limit: %s", limit)
        raise asyncio.LimitOverrunError("hit limit %s" % limit, count)

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


async def async_produce_tasks(limit: int = 0, incremental: bool = True):
    count = 0
    logger = setup_logger("producer")
    logger.info("started producer")

    for season in get_seasons(incremental):
        logger.info("enqueuing season: %s", season.year)
        for event in get_events(season.id, incremental):
            logger.info("enqueuing event: %s", event.short_name)
            for category in get_categories(event.id):
                logger.info("enqueuing category: %s", category.name)
                sessions = await async_get_sessions(event.id, category.id)
                tasks = [
                    asyncio.create_task(
                        do_task(
                            season,
                            event,
                            category,
                            x,
                            count,
                            limit,
                        )
                    )
                    for x in sessions
                ]
                count += len(tasks)
                logger.info("load item: %s/%s", count, limit if limit > 0 else "inf")

                await asyncio.gather(*tasks)


def produce_tasks(limit: int = 0, incremental: bool = True):
    count = 0
    logger = setup_logger("producer")
    logger.info("started producer")

    for season in get_seasons(incremental):
        logger.info("enqueuing season: %s", season.year)
        for event in get_events(season.id, incremental):
            logger.info("enqueuing event: %s", event.short_name)
            for category in get_categories(event.id):
                logger.info("enqueuing category: %s", category.name)
                for session in get_sessions(event.id, category.id):
                    if limit > 0 and count >= limit:
                        logger.info("producer hit limit: %s", limit)
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

    logger.info("finished producer")


async def main(limit: int, incremental: bool):
    await async_produce_tasks(limit=limit, incremental=incremental)


if __name__ == "__main__":
    args = []
    file, limit, load_type, *args = sys.argv
    if load_type == "inc":
        asyncio.run(main(limit=int(limit), incremental=True))
    else:
        asyncio.run(main(limit=int(limit), incremental=False))
