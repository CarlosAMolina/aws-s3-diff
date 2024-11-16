from compare import S3DataComparator
from config import get_config

FilePathNamesToCompare = tuple[str, str, str]


def run():
    config = get_config()
    S3DataComparator().run(config)


if __name__ == "__main__":
    run()
