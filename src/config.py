import datetime
import os
import re
from pathlib import Path

from constants import AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX
from constants import AWS_ACCOUNT_WITHOUT_MORE_FILES_PREFIX
from constants import FILE_NAME_S3_URIS
from constants import FOLDER_NAME_S3_RESULTS
from constants import MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS
from types_custom import S3Query


class Config:
    def __init__(self, directory_s3_results_path: Path, file_what_to_analyze_path: Path):
        self._directory_s3_results_path = directory_s3_results_path
        self._s3_uris_file_reader = _S3UrisFileReader(file_what_to_analyze_path)
        self._folder_name_buckets_results = self._get_folder_name_buckets_results()

    def get_aws_accounts(self) -> list[str]:
        path_to_check = self.get_local_path_directory_results_to_compare()
        result = os.listdir(path_to_check)
        result.sort()
        return result

    def get_aws_account_with_data_to_sync(self) -> str:
        for aws_account in self.get_aws_accounts():
            if aws_account.startswith(AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX):
                return aws_account
        raise ValueError("No aws account to sync")

    def get_aws_account_that_must_not_have_more_files(self) -> str:
        for aws_account in self.get_aws_accounts():
            if aws_account.startswith(AWS_ACCOUNT_WITHOUT_MORE_FILES_PREFIX):
                return aws_account
        raise ValueError("No aws account that must not have more files")

    def get_s3_queries(self) -> list[S3Query]:
        return self._s3_uris_file_reader.get_s3_queries()

    def get_bucket_names_to_analyze(self) -> list[str]:
        return self._s3_uris_file_reader.get_bucket_names_to_analyze()

    def get_local_path_directory_bucket_results(self, bucket_name: str) -> Path:
        return self._directory_s3_results_path.joinpath(self._folder_name_buckets_results, bucket_name)

    def get_local_path_directory_results_to_compare(self) -> Path:
        return self._directory_s3_results_path.joinpath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS)

    def get_local_path_file_query_results(self, s3_query: S3Query) -> Path:
        exported_files_directory_path = self.get_local_path_directory_bucket_results(s3_query.bucket)
        file_name_query_results = self._get_file_name_for_s3_path_name_results(s3_query.prefix)
        return exported_files_directory_path.joinpath(file_name_query_results)

    def _get_folder_name_buckets_results(self) -> str:
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    def _get_file_name_for_s3_path_name_results(self, s3_path_name: str) -> str:
        s3_path_name_clean = s3_path_name[:-1] if s3_path_name.endswith("/") else s3_path_name
        exported_file_name = s3_path_name_clean.replace("/", "-")
        return f"{exported_file_name}.csv"

    # TODO
    def get_s3_path_from_results_local_file(self, local_file_name: str) -> str:
        return local_file_name

    # TODO
    def _get_map_s3_path_and_local_file_results(self) -> dict[str, str]:
        return {}


class _S3UrisFileReader:
    def __init__(self, file_path: Path):
        self._file_what_to_analyze_path = file_path

    def get_s3_queries(self) -> list[S3Query]:
        return [
            S3Query(bucket, path_name)
            for bucket, path_names in self._get_dict_s3_uris_to_analyze().items()
            for path_name in path_names
        ]

    def get_bucket_names_to_analyze(self) -> list[str]:
        return list(self._get_dict_s3_uris_to_analyze().keys())

    def _get_dict_s3_uris_to_analyze(self) -> dict:
        result = {}
        with open(self._file_what_to_analyze_path) as f:
            for s3_uri in f.read().splitlines():
                bucket_name, file_path_name = self._get_bucket_and_path_from_s3_uri(s3_uri)
                if bucket_name not in result:
                    result[bucket_name] = []
                result[bucket_name].append(file_path_name)
            return result

    def _get_bucket_and_path_from_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        # https://stackoverflow.com/a/47130367
        match = re.match(r"s3:\/\/(?P<bucket_name>.+?)\/(?P<file_path>.+)", s3_uri)
        return match.group("bucket_name"), match.group("file_path")


def get_config() -> Config:
    current_path = Path(__file__).parent.absolute()
    directory_s3_results_path = current_path.parent.joinpath(FOLDER_NAME_S3_RESULTS)
    file_what_to_analyze_path = current_path.joinpath(FILE_NAME_S3_URIS)
    return Config(directory_s3_results_path, file_what_to_analyze_path)
