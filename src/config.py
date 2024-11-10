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
        return _S3KeyConverter().get_local_file_name_for_results_from_s3_uri_key(s3_path_name)

    # TODO
    def get_s3_path_from_results_local_file(self, local_file_name: str) -> str:
        return local_file_name

    # TODO
    def _get_map_s3_path_and_local_file_results(self) -> dict[str, str]:
        return {}


class _S3KeyConverter:
    def get_local_file_name_for_results_from_s3_uri_key(self, s3_uri_key: str) -> str:
        s3_uri_key_clean = s3_uri_key[:-1] if s3_uri_key.endswith("/") else s3_uri_key
        exported_file_name = s3_uri_key_clean.replace("/", "-")
        return f"{exported_file_name}.csv"


class _S3UrisFileReader:
    def __init__(self, file_path: Path):
        self._file_what_to_analyze_path = file_path

    def get_s3_queries(self) -> list[S3Query]:
        with open(self._file_what_to_analyze_path) as f:
            return [S3Query(_S3UriParts(s3_uri).bucket, _S3UriParts(s3_uri).key) for s3_uri in f.read().splitlines()]

    def get_bucket_names_to_analyze(self) -> list[str]:
        with open(self._file_what_to_analyze_path) as f:
            return list(set(_S3UriParts(s3_uri).bucket for s3_uri in f.read().splitlines()))


class _S3UriParts:
    def __init__(self, s3_uri: str):
        self._s3_uri = s3_uri

    @property
    def bucket(self) -> str:
        return self._get_regex_match_s3_uri_parts(self._s3_uri).group("bucket_name")

    @property
    def key(self) -> str:
        return self._get_regex_match_s3_uri_parts(self._s3_uri).group("object_key")

    def _get_regex_match_s3_uri_parts(self, s3_uri: str) -> re.Match:
        result = re.match(self._regex_s3_uri_parts, s3_uri)
        assert result is not None
        return result

    @property
    def _regex_s3_uri_parts(self) -> str:
        """https://stackoverflow.com/a/47130367"""
        return r"s3:\/\/(?P<bucket_name>.+?)\/(?P<object_key>.+)"


def get_config() -> Config:
    current_path = Path(__file__).parent.absolute()
    directory_s3_results_path = current_path.parent.joinpath(FOLDER_NAME_S3_RESULTS)
    file_what_to_analyze_path = current_path.joinpath(FILE_NAME_S3_URIS)
    return Config(directory_s3_results_path, file_what_to_analyze_path)
