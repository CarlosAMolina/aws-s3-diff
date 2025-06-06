import json
import re
from pathlib import Path

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv

from aws_s3_diff.exceptions import AnalysisConfigError
from aws_s3_diff.local_results import LocalPaths
from aws_s3_diff.types_custom import S3Query

FILE_NAME_ANALYSIS_CONFIG = "analysis-config.json"
FILE_NAME_S3_URIS_TO_ANALYZE = "s3-uris-to-analyze.csv"
# S3 uri regex: https://stackoverflow.com/a/47130367
REGEX_BUCKET_PREFIX_FROM_S3_URI = r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)"


class AnalysisConfigChecker:
    def __init__(self):
        self._analysis_config_reader = AnalysisConfigReader()
        self._s3_uris_file_reader = S3UrisFileReader()

    def assert_file_is_correct(self):
        self._assert_account_origin_exists()
        self._assert_accounts_target_exist()

    def _assert_account_origin_exists(self):
        account_origin = self._analysis_config_reader.get_account_origin()
        if not self._exists_account(account_origin):
            raise AnalysisConfigError(self._get_error_message_account_does_not_exist(account_origin))

    def _assert_accounts_target_exist(self):
        accounts_wrong_check_copy = self._get_accounts_not_exist(
            self._analysis_config_reader.get_accounts_where_files_must_be_copied()
        )
        accounts_wrong_check_more_files = self._get_accounts_not_exist(
            self._analysis_config_reader.get_accounts_that_must_not_have_more_files()
        )
        accounts_wrong = accounts_wrong_check_copy | accounts_wrong_check_more_files
        if len(accounts_wrong) == 1:
            raise AnalysisConfigError(self._get_error_message_account_does_not_exist(list(accounts_wrong)[0]))
        if len(accounts_wrong) > 1:
            raise AnalysisConfigError(self._get_error_message_accounts_do_not_exist(sorted(accounts_wrong)))

    def _exists_account(self, account: str) -> bool:
        return account in self._s3_uris_file_reader.get_accounts()

    def _get_accounts_not_exist(self, accounts: list[str]) -> set[str]:
        return {account for account in accounts if not self._exists_account(account)}

    def _get_error_message_account_does_not_exist(self, account: str) -> str:
        return self._get_error_message(f"The AWS account '{account}' is")

    def _get_error_message_accounts_do_not_exist(self, accounts: list[str]) -> str:
        accounts_str = "', '".join(accounts)
        return self._get_error_message(f"The AWS accounts '{accounts_str}' are")

    def _get_error_message(self, text_prefix: str) -> str:
        return f"{text_prefix} defined in {FILE_NAME_ANALYSIS_CONFIG} but not in {FILE_NAME_S3_URIS_TO_ANALYZE}"


# TODO testing: not the file in the config folder, create one in for the tests
class AnalysisConfigReader:
    def __init__(self):
        self._config_directory_path = LocalPaths().config_directory
        self.__analysis_config = None  # To avoid read a file in __init__.

    def must_run_analysis(self) -> bool:
        return self._analysis_config["run_analysis"] is True

    def get_account_origin(self) -> str:
        return self._analysis_config["origin"]

    def get_accounts_that_must_not_have_more_files(self) -> list[str]:
        return self._analysis_config["can_the_file_exist_in"]

    def get_accounts_where_files_must_be_copied(self) -> list[str]:
        return self._analysis_config["is_the_file_copied_to"]

    @property
    def _analysis_config(self) -> dict:
        if self.__analysis_config is None:
            self.__analysis_config = self._get_analysis_config()
        return self.__analysis_config

    def _get_analysis_config(self) -> dict:
        with open(self._file_path_what_to_analyze, encoding="utf-8") as read_file:
            return json.load(read_file)

    @property
    def _file_path_what_to_analyze(self) -> Path:
        return self._config_directory_path.joinpath(FILE_NAME_ANALYSIS_CONFIG)


class S3UrisFileChecker:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def assert_file_is_correct(self):
        self._assert_no_empty_account()
        self._assert_no_empty_uris()
        self._assert_no_duplicated_uri_per_account()

    def _assert_no_empty_account(self):
        if any(account.startswith("Unnamed: ") for account in self._s3_uris_file_reader.get_accounts()):
            raise ValueError("Some AWS account names are empty")

    def _assert_no_empty_uris(self):
        if self._s3_uris_file_reader.is_any_uri_null():
            raise ValueError("Some URIs are empty")

    def _assert_no_duplicated_uri_per_account(self):
        for account in self._s3_uris_file_reader.get_accounts():
            queries = self._s3_uris_file_reader.get_s3_queries_for_account(account)
            if len(queries) != len(set(queries)):
                raise ValueError(f"The AWS account {account} has duplicated URIs")


class S3UrisFileReader:
    def __init__(self):
        self._config_directory_path = LocalPaths().config_directory
        self.__df_file_what_to_analyze = None  # To avoid read a file in __init__.

    def get_accounts(self) -> list[str]:
        return self.file_df.columns.to_list()

    def get_first_account(self) -> str:
        return self.get_accounts()[0]

    def get_last_account(self) -> str:
        return self.get_accounts()[-1]

    def get_s3_queries_for_account(self, account: str) -> list[S3Query]:
        s3_uris_to_analyze = self.file_df[account].to_list()
        return [self.get_s3_query_from_s3_uri(s3_uri) for s3_uri in s3_uris_to_analyze]

    def get_s3_query_from_s3_uri(self, s3_uri: str) -> S3Query:
        return S3Query(_S3UriParts(s3_uri).bucket, _S3UriParts(s3_uri).key)

    def get_df_s3_uris_map_between_accounts(self, account_origin: str, account_target: str) -> Df:
        return self.file_df[[account_origin, account_target]]

    def is_any_uri_null(self) -> np.bool:
        return self.file_df.isnull().values.any()

    @property
    def file_df(self) -> Df:
        if self.__df_file_what_to_analyze is None:
            self.__df_file_what_to_analyze = self._get_df_file_what_to_analyze()
        return self.__df_file_what_to_analyze

    def _get_df_file_what_to_analyze(self) -> Df:
        return read_csv(self._file_path_what_to_analyze)

    @property
    def _file_path_what_to_analyze(self) -> Path:
        return self._config_directory_path.joinpath(FILE_NAME_S3_URIS_TO_ANALYZE)


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
        result = re.match(REGEX_BUCKET_PREFIX_FROM_S3_URI, s3_uri)
        assert result is not None
        return result
