import re

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv
from pandas import Series

from aws_s3_diff.config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
from aws_s3_diff.s3_data.interface import CsvExporter
from aws_s3_diff.s3_data.interface import CsvReader
from aws_s3_diff.s3_data.interface import DataGenerator
from aws_s3_diff.s3_data.one_account import AccountCsvReader
from aws_s3_diff.s3_data.one_account import OriginS3UrisAsIndexAccountDfModifier

_logger = get_logger()


class AccountsCsvExporter(CsvExporter):
    def __init__(self):
        self._local_results = LocalResults()

    def export_df(self, df: Df):
        file_path = self._local_results.get_file_path_all_accounts()
        _logger.info(f"Exporting {file_path}")
        df.to_csv(index=False, path_or_buf=file_path)


class AccountsCsvReader(CsvReader):
    def __init__(self):
        self._accounts_cache = None
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> Df:
        result = self._get_df_from_csv()
        result.columns = MultiIndex.from_tuples(self._get_multi_index_tuples_for_df_columns(result.columns))
        return result

    def _get_df_from_csv(self) -> Df:
        return read_csv(
            self._local_results.get_file_path_all_accounts(),
            index_col=[f"bucket_{self._accounts[0]}", f"file_path_in_s3_{self._accounts[0]}", "file_name_all_accounts"],
            parse_dates=[f"{account}_date" for account in self._accounts],
        ).astype({f"{account}_size": "Int64" for account in self._accounts})

    def _get_multi_index_tuples_for_df_columns(self, columns: Index) -> list[tuple[str, str]]:
        return [self._get_multi_index_from_column_name(column_name) for column_name in columns]

    def _get_multi_index_from_column_name(self, column_name: str) -> tuple[str, str]:
        for account in self._accounts:
            regex_result = re.match(rf"{account}_(?P<key>.*)", column_name)
            if regex_result is not None:
                return account, regex_result.group("key")
        raise ValueError(f"Not managed column name: {column_name}")

    @property
    def _accounts(self) -> list[str]:
        if self._accounts_cache is None:
            self._accounts_cache = self._s3_uris_file_reader.get_accounts()
        return self._accounts_cache


class AccountsDataGenerator(DataGenerator):
    def __init__(self):
        self._accounts_cache = None
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> Df:
        result = self._get_df_combine_accounts_s3_data()
        result = self._get_df_set_all_queries_despite_without_results(result)
        self._get_df_set_columns_as_single_index(result)
        return result.reset_index(
            names=[
                f"bucket_{self._account_origin}",
                f"file_path_in_s3_{self._account_origin}",
                "file_name_all_accounts",
            ]
        )

    def _get_df_combine_accounts_s3_data(self) -> Df:
        account_origin_df = AccountCsvReader(self._account_origin).get_df()
        account_target_df_array = self._get_array_df_account_target_to_combine()
        result = account_origin_df.join(account_target_df_array, how="outer")
        return result.dropna(axis="index", how="all")

    def _get_array_df_account_target_to_combine(self) -> list[Df]:
        result = []
        for account in self._account_targets:
            account_df = AccountCsvReader(account).get_df()
            account_df_to_join = OriginS3UrisAsIndexAccountDfModifier(self._account_origin, account).get_df_modified(
                account_df
            )
            result.append(account_df_to_join)
        return result

    def _get_df_set_all_queries_despite_without_results(self, df: Df) -> Df:
        result = self._get_empty_df_original_account_queries_as_index()
        result.columns = MultiIndex.from_arrays([[], []])  # To merge with a MultiIndex columns Df.
        result = result.join(df.reset_index("name"))
        return result.set_index("name", append=True)

    def _get_empty_df_original_account_queries_as_index(self) -> Df:
        result = self._s3_uris_file_reader.get_df_file_for_account(self._account_origin)
        # TODO refactor extract function, this line is done in other files.
        result = self._get_df_uri_parts(result)
        result.columns = ["bucket", "prefix"]
        # TODO refactor extract function, this line is done in other files.
        result = self._get_df_add_last_slash_to_values(result, "prefix")
        return result.set_index(["bucket", "prefix"])

    def _get_df_uri_parts(self, series: Series) -> Df:
        return series.str.extract(REGEX_BUCKET_PREFIX_FROM_S3_URI, expand=False)

    def _get_df_add_last_slash_to_values(self, df: Df, column_name: str) -> Df:
        result = df
        result.loc[~result[column_name].str.endswith("/"), column_name] = result[column_name] + "/"
        return result

    def _get_df_set_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)

    @property
    def _account_origin(self) -> str:
        return self._accounts[0]

    @property
    def _account_targets(self) -> list[str]:
        return [account for account in self._accounts if account != self._account_origin]

    @property
    def _accounts(self) -> list[str]:
        if self._accounts_cache is None:
            self._accounts_cache = self._s3_uris_file_reader.get_accounts()
        return self._accounts_cache
