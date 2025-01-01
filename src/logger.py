import logging


def get_logger():
    logger = logging.getLogger(__name__)
    logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.INFO)
    return logger
