from pathlib import Path

from src.config import _S3UrisFile
from src.config import Config
from src.constants import FOLDER_NAME_S3_RESULTS


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    aws_account = "aws_account_1_pro"
    directory_s3_results_path = current_path.joinpath("fake-files", FOLDER_NAME_S3_RESULTS)
    file_what_to_analyze_path = current_path.joinpath(_S3UrisFile._FILE_NAME_S3_URIS)
    return Config(aws_account, directory_s3_results_path, file_what_to_analyze_path)
