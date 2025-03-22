import re
from pathlib import Path

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from config_files import S3UrisFileReader
from local_results import LocalResults
from s3_data.interface import AsMultiIndexFactory
from s3_data.interface import AsSingleIndexFactory
from s3_data.interface import CsvCreator
from s3_data.interface import CsvReader
from s3_data.interface import FileNameCreator
from s3_data.interface import FromCsvDfFactory
from s3_data.interface import MultiIndexDfCreator
from s3_data.interface import NewDfFactory
from s3_data.interface import SimpleIndexDfCreator
from s3_data.one_account import AccountDf
from s3_data.one_account import AccountFromCsvDfFactory
from types_custom import MultiIndexDf


# TODO
class _AccountsMultiIndexDfCreator(MultiIndexDfCreator):
    pass


class AccountsCsvCreator(CsvCreator):
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        return _AccountsSimpleIndexDfCreator()

    def _get_file_name_creator(self) -> FileNameCreator:
        return _AccountsFileNameCreator()


class AccountsFromCsvDfFactory(FromCsvDfFactory):
    def __init__(self):
        self._accounts_as_multi_index_factory = _AccountsAsMultiIndexFactory()
        self._accounts_simple_index_df_creator = _AccountsSimpleIndexDfCreator()

    def get_df(self) -> MultiIndexDf:
        result = self._accounts_simple_index_df_creator.get_df()
        return self._accounts_as_multi_index_factory.get_df(result)


class _AccountsSimpleIndexDfCreator(SimpleIndexDfCreator):
    def __init__(self):
        self._accounts_as_single_index_factory = _AccountsAsSingleIndexFactory()
        self._df_from_csv_creator = _AccountsCsvReader()
        self._local_results = LocalResults()
        # TODO deprecate these classes, rename factory
        self._new_df_creator = _AccountsNewDfFactory()

    def get_df(self) -> Df:
        if self._get_file_path().is_file():
            return self._get_df_from_csv()
        return self._get_df_create_new()

    def _get_df_from_csv(self) -> Df:
        return self._df_from_csv_creator.get_df()

    def _get_df_create_new(self) -> Df:
        df = self._new_df_creator.get_df()
        return self._accounts_as_single_index_factory.get_df(df)

    # TODO refator, code duplicated in other files (in this file too)
    def _get_file_path(self) -> Path:
        # TODO avoid access values of attribute of a class
        return self._local_results.analysis_paths.directory_analysis.joinpath(
            self._get_file_name_creator().get_file_name()
        )

    def _get_file_name_creator(self) -> FileNameCreator:
        return _AccountsFileNameCreator()


class _AccountsFileNameCreator(FileNameCreator):
    # TODO deprecate file_s3_data_all_accounts with this
    def get_file_name(self) -> str:
        return "s3-files-all-accounts.csv"


class _AccountsNewDfFactory(NewDfFactory):
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> MultiIndexDf:
        result = self._get_df_merge_accounts_s3_data()
        return self._get_df_set_all_queries_despite_without_results(result)

    def _get_df_merge_accounts_s3_data(self) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        result = AccountFromCsvDfFactory(accounts[0]).get_df()
        for account in accounts[1:]:
            account_df = AccountFromCsvDfFactory(account).get_df_with_original_account_index()
            result = AccountDf(result).join(account_df)
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


class _AccountsAsSingleIndexFactory(AsSingleIndexFactory):
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        account_1 = self._s3_uris_file_reader.get_first_account()
        return result.reset_index(
            names=[
                f"bucket_{account_1}",
                f"file_path_in_s3_{account_1}",
                "file_name_all_accounts",
            ]
        )


class _AccountsAsMultiIndexFactory(AsMultiIndexFactory):
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self, df: Df) -> MultiIndexDf:
        return self._get_df_set_multi_index_columns(df)

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


class _AccountsCsvReader(CsvReader):
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
        # TODO avoid access values of attribute of a class
        return self._local_results.analysis_paths.directory_analysis.joinpath(
            self._get_file_name_creator().get_file_name()
        )

    def _get_file_name_creator(self) -> FileNameCreator:
        return _AccountsFileNameCreator()
