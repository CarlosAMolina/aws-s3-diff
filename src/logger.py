import logging


def get_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)
    logging.basicConfig(format="[%(levelname)s] %(message)s", level=logging.DEBUG)
    # Avoid other modules logs: https://stackoverflow.com/a/49814387
    for logger_name in ("boto3", "botocore", "s3transfer"):
        logging.getLogger(logger_name).setLevel(logging.ERROR)
    return logger
