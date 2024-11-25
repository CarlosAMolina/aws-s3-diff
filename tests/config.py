from pathlib import Path

from src.config import _AwsAccountS3UrisFileReader
from src.config import _S3UrisFile
from src.config import Config


def get_config_for_the_test() -> Config:
    current_path = Path(__file__).parent.absolute()
    aws_account = "aws_account_1_pro"
    result = Config(aws_account)
    result._s3_uris_file_reader = _AwsAccountS3UrisFileReader(aws_account)
    result._s3_uris_file_reader._file_what_to_analyze_path = current_path.joinpath(
        "fake-files", _S3UrisFile._FILE_NAME_S3_URIS
    )
    return result
