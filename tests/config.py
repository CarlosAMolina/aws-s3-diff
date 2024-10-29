from pathlib import Path

from src.config import Config
from src.constants import FILE_NAME_S3_URIS


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    file_name_what_to_analyze = current_path.joinpath(FILE_NAME_S3_URIS)
    return Config(file_name_what_to_analyze)
