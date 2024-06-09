import asyncio
import sys


from motogp.model import TaskQueue
from motogp.processing import consumer, export_results, load_queue, setup_logger


async def process(incremental: bool = True):
    logger = setup_logger()
    logger.info("started")

    load_queue(incremental=False)
    queue = asyncio.Queue()
    for task in TaskQueue.from_db().tasks:
        queue.put_nowait(task)

    consumers = [asyncio.create_task(consumer(queue)) for _ in range(10)]

    await queue.join()

    for c in consumers:
        c.cancel()

    # export
    export_results()

    logger.info("finished")


args = []
file, load_type, *args = sys.argv()
if load_type == "inc":
    asyncio.run(process(incremental=True))
else:
    asyncio.run(process(incremental=False))
