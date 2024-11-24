from pathlib import Path

from src.config import _S3UrisFile
from src.config import Config
from src.constants import FOLDER_NAME_S3_RESULTS


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    aws_account = "aws_account_1_pro"
    file_what_to_analyze_path = current_path.joinpath("fake-files", _S3UrisFile._FILE_NAME_S3_URIS)
    result = Config(aws_account, file_what_to_analyze_path)
    directory_s3_results_path = current_path.joinpath("fake-files", FOLDER_NAME_S3_RESULTS)
    result._directory_s3_results_path = directory_s3_results_path
    return result
