import logging
import sys


def setup_logger(name: str):
    file_handler = logging.FileHandler(filename=name + ".log")
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
        handlers=handlers,
    )

    return logging.getLogger(name)
