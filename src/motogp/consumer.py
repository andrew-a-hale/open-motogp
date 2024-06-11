import asyncio
import sys

import httpx

from motogp.database import export_results
from motogp.logger import setup_logger
from motogp.model import TaskStatus, TaskQueue
from motogp.endpoints import async_get_classification


class SyncError(Exception):
    pass


# scrape
async def consumer(queue: asyncio.Queue):
    while True:
        logger = setup_logger("consumer")
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
            try:
                classification.sync()
                task.upsert_status(TaskStatus.COMPLETED)
            except Exception as err:
                logger.error(
                    "failed to sync classification with session_id: %s",
                    task.session_id,
                )
                task.upsert_status(TaskStatus.ERROR)

        queue.task_done()


async def main(limit: int):
    logger = setup_logger("consumer")
    logger.info("started consumer")

    queue = asyncio.Queue()
    for i, task in enumerate(TaskQueue.from_db(TaskStatus.QUEUED).tasks):
        queue.put_nowait(task)
        task.upsert_status(TaskStatus.QUEUED)

        if i > limit and limit > 0:
            break

    consumers = [asyncio.create_task(consumer(queue)) for _ in range(5)]

    await queue.join()

    for c in consumers:
        c.cancel()

    export_results()

    logger.info("finished consumer")


if __name__ == "__main__":
    args = []
    file, limit, load_type, *args = sys.argv
    if load_type == "inc":
        asyncio.run(main(limit=int(limit)))
    else:
        asyncio.run(main(limit=int(limit)))
