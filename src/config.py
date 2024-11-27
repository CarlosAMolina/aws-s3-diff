import re
from pathlib import Path

from pandas import DataFrame as Df
from pandas import read_csv

from analysis import get_aws_account_that_must_not_have_more_files
from analysis import get_aws_account_with_data_to_sync
from types_custom import S3Query


class Config:
    pass

    def get_aws_account_with_data_to_sync(self) -> str:
        return get_aws_account_with_data_to_sync()

    def get_aws_account_that_must_not_have_more_files(self) -> str:
        return get_aws_account_that_must_not_have_more_files()


class _S3UrisFile:
    _FILE_NAME_S3_URIS = "s3-uris-to-analyze.csv"

    @property
    def file_path(self) -> Path:
        current_path = Path(__file__).parent.absolute()
        return current_path.joinpath(self._FILE_NAME_S3_URIS)


class S3UrisFileReader:
    def __init__(self):
        self._file_what_to_analyze_path = _S3UrisFile().file_path

    def get_aws_accounts(self) -> list[str]:
        return self._get_df_file_what_to_analyze().columns.to_list()

    def get_number_of_aws_accounts(self) -> int:
        return len(self.get_aws_accounts())

    def _get_df_file_what_to_analyze(self) -> Df:
        return read_csv(self._file_what_to_analyze_path)


class AwsAccountS3UrisFileReader(S3UrisFileReader):
    def __init__(self, aws_account: str):
        self._aws_account = aws_account
        super().__init__()

    def get_s3_queries(self) -> list[S3Query]:
        return [
            S3Query(_S3UriParts(s3_uri).bucket, _S3UriParts(s3_uri).key) for s3_uri in self._get_s3_uris_to_analyze()
        ]

    def _get_s3_uris_to_analyze(self) -> list[str]:
        return self._get_df_file_what_to_analyze()[self._aws_account].to_list()


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
