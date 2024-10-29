from pathlib import Path

from src.config import Config
from src.constants import FILE_NAME_S3_URIS
from src.constants import FOLDER_NAME_S3_RESULTS


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    directory_s3_results_path = current_path.joinpath(FOLDER_NAME_S3_RESULTS)
    file_what_to_analyze_path = current_path.joinpath(FILE_NAME_S3_URIS)
    return Config(directory_s3_results_path, file_what_to_analyze_path)
