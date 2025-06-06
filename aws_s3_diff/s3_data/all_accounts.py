import re
from pathlib import Path

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from aws_s3_diff.config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.local_results import ACCOUNTS_FILE_NAME
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.s3_data.interface import CsvCreator
from aws_s3_diff.s3_data.interface import FromCsvDfCreator
from aws_s3_diff.s3_data.interface import FromMultiSimpleIndexDfCreator
from aws_s3_diff.s3_data.interface import FromSimpleMultiIndexDfCreator
from aws_s3_diff.s3_data.interface import NewDfCreator
from aws_s3_diff.s3_data.interface import SimpleIndexDfCreator
from aws_s3_diff.s3_data.one_account import AccountDf
from aws_s3_diff.types_custom import MultiIndexDf


# TODO deprecate
class AccountsDf:
    def __init__(self):
        self._accounts_simple_index_df_creator = _AccountsSimpleIndexDfCreator()

    def get_df(self) -> MultiIndexDf:
        result = self._accounts_simple_index_df_creator.get_df()
        # TODO initialize in __init__
        return _AccountsFromSimpleMultiIndexDfCreator(result).get_df()


class AccountsCsvCreator(CsvCreator):
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        return _AccountsSimpleIndexDfCreator()

    @property
    def _file_name(self) -> str:
        return ACCOUNTS_FILE_NAME


class _AccountsSimpleIndexDfCreator(SimpleIndexDfCreator):
    def __init__(self):
        self._df_from_csv_creator = _AccountsFromCsvDfCreator()
        self._local_results = LocalResults()
        # TODO deprecate these classes
        self._new_df_creator = _AccountsNewDfCreator()

    def get_df(self) -> Df:
        if self._get_file_path().is_file():
            return self._get_df_from_csv()
        return self._get_df_create_new()

    def _get_df_from_csv(self) -> Df:
        return self._df_from_csv_creator.get_df()

    def _get_df_create_new(self) -> Df:
        df = self._new_df_creator.get_df()
        # TODO initialize in __init__
        return _AccountsFromMultiSimpleIndexDfCreator(df).get_df()

    # TODO refator, code duplicated in other files (in this file too)
    def _get_file_path(self) -> Path:
        return self._local_results.get_file_path_results(ACCOUNTS_FILE_NAME)


class _AccountsNewDfCreator(NewDfCreator):
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> MultiIndexDf:
        result = self._get_df_merge_accounts_s3_data()
        return self._get_df_set_all_queries_despite_without_results(result)

    def _get_df_merge_accounts_s3_data(self) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        account_df_array = [AccountDf().get_account_df_to_join(account, accounts[0]) for account in accounts]
        result = account_df_array[0].join(account_df_array[1:], how="outer")
        return result.dropna(axis="index", how="all")

    def _get_df_set_all_queries_despite_without_results(self, df: Df) -> Df:
        result = self._get_empty_df_original_account_queries_as_index()
        result.columns = MultiIndex.from_arrays([[], []])  # To merge with a MultiIndex columns Df.
        result = result.join(df.reset_index("name"))
        return result.set_index("name", append=True)

    def _get_empty_df_original_account_queries_as_index(self) -> Df:
        result = self._s3_uris_file_reader.file_df[self._s3_uris_file_reader.get_first_account()]
        # TODO refactor extract function, this line is done in other files.
        result = result.str.extract(REGEX_BUCKET_PREFIX_FROM_S3_URI, expand=False)
        result.columns = ["bucket", "prefix"]
        # TODO refactor extract function, this line is done in other files.
        result.loc[~result["prefix"].str.endswith("/"), "prefix"] = result["prefix"] + "/"
        return result.set_index(["bucket", "prefix"])


class _AccountsFromMultiSimpleIndexDfCreator(FromMultiSimpleIndexDfCreator):
    def __init__(self, df: Df):
        self._s3_uris_file_reader = S3UrisFileReader()
        super().__init__(df)

    def get_df(self) -> Df:
        result = self._df.copy()
        self._set_df_columns_as_single_index(result)
        account_1 = self._s3_uris_file_reader.get_first_account()
        return result.reset_index(
            names=[
                f"bucket_{account_1}",
                f"file_path_in_s3_{account_1}",
                "file_name_all_accounts",
            ]
        )


class _AccountsFromSimpleMultiIndexDfCreator(FromSimpleMultiIndexDfCreator):
    def __init__(self, df: Df):
        self._df = df
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> MultiIndexDf:
        return self._get_df_set_multi_index_columns(self._df)

    def _get_df_set_multi_index_columns(self, df: Df) -> Df:
        result = df
        result.columns = MultiIndex.from_tuples(self._get_multi_index_tuples_for_df_columns(result.columns))
        return result

    def _get_multi_index_tuples_for_df_columns(self, columns: Index) -> list[tuple[str, str]]:
        return [self._get_multi_index_from_column_name(column_name) for column_name in columns]

    def _get_multi_index_from_column_name(self, column_name: str) -> tuple[str, str]:
        for account in self._s3_uris_file_reader.get_accounts():
            regex_result = re.match(rf"{account}_(?P<key>.*)", column_name)
            if regex_result is not None:
                return account, regex_result.group("key")
        raise ValueError(f"Not managed column name: {column_name}")


class _AccountsFromCsvDfCreator(FromCsvDfCreator):
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    # TODO extract common code with _AccountSimpleIndexDfCreator._get_df_from_csv
    def get_df(self) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        return read_csv(
            self._get_file_path(),
            index_col=[f"bucket_{accounts[0]}", f"file_path_in_s3_{accounts[0]}", "file_name_all_accounts"],
            parse_dates=[f"{account}_date" for account in accounts],
        ).astype({f"{account}_size": "Int64" for account in accounts})

    # TODO refator, code duplicated in other files
    def _get_file_path(self) -> Path:
        return self._local_results.get_file_path_results(ACCOUNTS_FILE_NAME)
