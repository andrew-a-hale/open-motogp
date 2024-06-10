import asyncio
import sys


from motogp.model import TaskQueue, TaskStatus
from motogp.processing import consumer, export_results, load_queue, setup_logger


async def process(limit: int, incremental: bool):
    logger = setup_logger()
    logger.info("started")

    load_queue(limit=limit, incremental=incremental)
    queue = asyncio.Queue()
    for i, task in enumerate(TaskQueue.from_db(TaskStatus.QUEUED).tasks):
        queue.put_nowait(task)

        if i > limit:
            break

    consumers = [asyncio.create_task(consumer(queue)) for _ in range(10)]

    await queue.join()

    for c in consumers:
        c.cancel()

    # export
    export_results()

    logger.info("finished")


if __name__ == "__main__":
    args = []
    file, limit, load_type, *args = sys.argv
    if load_type == "inc":
        asyncio.run(process(limit=int(limit), incremental=True))
    else:
        asyncio.run(process(limit=int(limit), incremental=False))
