import re
from pathlib import Path

from pandas import DataFrame as Df
from pandas import read_csv

from types_custom import S3Query


class S3UrisFileReader:
    _FILE_NAME_S3_URIS = "s3-uris-to-analyze.csv"

    def get_aws_accounts(self) -> list[str]:
        return self.get_df_file_what_to_analyze().columns.to_list()

    def get_number_of_aws_accounts(self) -> int:
        return len(self.get_aws_accounts())

    def get_s3_queries_for_aws_account(self, aws_account: str) -> list[S3Query]:
        s3_uris_to_analyze = self.get_df_file_what_to_analyze()[aws_account].to_list()
        return [self.get_s3_query_from_s3_uri(s3_uri) for s3_uri in s3_uris_to_analyze]

    def get_s3_query_from_s3_uri(self, s3_uri: str) -> S3Query:
        return S3Query(_S3UriParts(s3_uri).bucket, _S3UriParts(s3_uri).key)

    def get_df_file_what_to_analyze(self) -> Df:
        return read_csv(self._file_what_to_analyze_path)

    # TODO rename to _file_path_what_to_analyze
    @property
    def _file_what_to_analyze_path(self) -> Path:
        return self._directory_path_what_to_analyze.joinpath(self._FILE_NAME_S3_URIS)

    @property
    def _directory_path_what_to_analyze(self) -> Path:
        return Path(__file__).parent.absolute()


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
