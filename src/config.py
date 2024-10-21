import os
import re
from pathlib import Path

from constants import AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX
from constants import FILE_NAME_S3_URIS
from constants import MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS


class Config:
    def __init__(self, path_config_files: Path, path_with_folder_exported_s3_data: Path):
        self._path_config_files = path_config_files
        self._path_with_folder_exported_s3_data = path_with_folder_exported_s3_data

    def get_aws_accounts(self) -> list[str]:
        path_to_check = self.get_path_exported_s3_data()
        result = os.listdir(path_to_check)
        result.sort()
        return result

    def get_aws_account_with_data_to_sync(self) -> str:
        for aws_account in self.get_aws_accounts():
            if aws_account.startswith(AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX):
                return aws_account
        raise ValueError("No aws account to sync")

    def get_dict_s3_uris_to_analyze(self) -> dict:
        _file_name_what_to_analyze = self._path_config_files.joinpath(FILE_NAME_S3_URIS)
        result = {}
        with open(_file_name_what_to_analyze) as f:
            for s3_uri in f.read().splitlines():
                bucket_name, file_path_name = _get_bucket_and_path_from_s3_uri(s3_uri)
                if bucket_name not in result:
                    result[bucket_name] = []
                result[bucket_name].append(file_path_name)
            return result

    def get_path_exported_s3_data(self) -> Path:
        return self._path_with_folder_exported_s3_data.joinpath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS)


def _get_bucket_and_path_from_s3_uri(s3_uri: str) -> tuple[str, str]:
    # https://stackoverflow.com/a/47130367
    match = re.match(r"s3:\/\/(.+?)\/(.+)", s3_uri)
    bucket_name = match.group(1)
    file_path = match.group(2)
    return bucket_name, file_path
    # TODO regex_date = r"s3://(?P<year>\d{2})/(?P<month>\d{2}).xlsx"
