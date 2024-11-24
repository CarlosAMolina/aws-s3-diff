from pathlib import Path

from src.config import _AwsAccountS3UrisFileReader
from src.config import _S3UrisFile
from src.config import Config
from src.constants import FOLDER_NAME_S3_RESULTS
from src.local_results import LocalResults


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    aws_account = "aws_account_1_pro"
    local_results = LocalResults()
    local_results.path_directory_all_results = lambda: current_path.joinpath(
        "fake-files", FOLDER_NAME_S3_RESULTS, "exports-all-aws-accounts"
    )
    result = Config(aws_account, local_results)
    result._directory_s3_results_path = current_path.joinpath("fake-files", FOLDER_NAME_S3_RESULTS)
    result._s3_uris_file_reader = _AwsAccountS3UrisFileReader(aws_account)
    result._s3_uris_file_reader._file_what_to_analyze_path = current_path.joinpath(
        "fake-files", _S3UrisFile._FILE_NAME_S3_URIS
    )
    return result
