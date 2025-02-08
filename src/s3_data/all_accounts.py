import re
from abc import ABC
from abc import abstractmethod

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data.interface import AsMultiIndexModifier
from s3_data.interface import CsvReader
from s3_data.one_account import AccountS3DataFromCsvFactory
from types_custom import MultiIndexDf

# TODO apply refactors done in one_account.py:
# - extract S3DataFromCsvFactory (make pulic _AccountsS3DataCsvReader)
# - extract _MultiIndexDfFactory
# - etc


class AllAccountsS3DataFactory:
    def __init__(self):
        self._accounts_s3_data_merger = _AccountsS3DataMerger()
        self._accounts_s3_data_csv_reader = _AccountsS3DataCsvReader()
        self._accounts_s3_data_transformer = _AccountsS3DataTransformer()
        self._local_results = LocalResults()
        self._logger = get_logger()

    def to_csv(self):
        # TODO no access property of property.
        file_path = self._local_results.analysis_paths.file_s3_data_all_accounts
        self._logger.info(f"Exporting all AWS accounts S3 files information to {file_path}")
        df = self._get_df_merging_each_account_s3_data()
        csv_df = self._accounts_s3_data_transformer.get_df_to_export(df)
        csv_df.to_csv(file_path)

    def get_df_from_csv(self) -> MultiIndexDf:
        return self._accounts_s3_data_csv_reader.get_df()

    def _get_df_merging_each_account_s3_data(self) -> MultiIndexDf:
        return self._accounts_s3_data_merger.get_df_merge_each_account_results()


class S3DataTransformer(ABC):
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    @abstractmethod
    def get_df_to_export(self, df: Df) -> Df:
        pass

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)

    def _set_df_index_column_names(self, df: Df):
        account_1 = self._s3_uris_file_reader.get_first_account()
        df.index.names = [
            f"bucket_{account_1}",
            f"file_path_in_s3_{account_1}",
            "file_name_all_accounts",
        ]


class _AccountsS3DataTransformer(S3DataTransformer):
    def get_df_to_export(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        self._set_df_index_column_names(result)
        return result


class _AccountsS3DataCsvReader:
    def __init__(self):
        self._s3_accounts_csv_reader = _AccountsCsvReader()
        self._accounts_as_multi_index_modifier = _AccountsAsMultiIndexModifier()

    def get_df(self) -> MultiIndexDf:
        result = self._s3_accounts_csv_reader.get_df()
        return self._accounts_as_multi_index_modifier.get_df(result)


class _AccountsAsMultiIndexModifier(AsMultiIndexModifier):
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

    # TODO extract common code with _AccountCsvReader
    def get_df(self) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        return read_csv(
            self._local_results.analysis_paths.file_s3_data_all_accounts,
            index_col=[f"bucket_{accounts[0]}", f"file_path_in_s3_{accounts[0]}", "file_name_all_accounts"],
            parse_dates=[f"{account}_date" for account in accounts],
        ).astype({f"{account}_size": "Int64" for account in accounts})


class _AccountsS3DataMerger:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_merge_each_account_results(self) -> MultiIndexDf:
        result = self._get_df_merge_accounts_s3_data()
        return self._get_df_set_all_queries_despite_without_results(result)

    def _get_df_merge_accounts_s3_data(self) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        result = AccountS3DataFromCsvFactory(accounts[0]).get_df()
        for account in accounts[1:]:
            account_df = AccountS3DataFromCsvFactory(account).get_df_with_original_account_index()
            result = result.join(account_df, how="outer")
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
