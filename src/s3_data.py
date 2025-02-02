import re
from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_client import S3Client
from types_custom import AccountS3DataDf
from types_custom import AllAccountsS3DataDf
from types_custom import FileS3Data
from types_custom import S3Data
from types_custom import S3Query


class AllAccountsS3DataFactory:
    def __init__(self):
        self._df_generator = _AccountsS3DataDfCombinator()
        self._export_to_csv = _CombinedAccountsS3DataDfToCsv().export
        self._local_results = LocalResults()

    def to_csv(self):
        df = self._df_generator.get_df()
        self._export_to_csv(df)

    def get_df_from_csv(self) -> AllAccountsS3DataDf:
        # TODO don't access a property of a property
        file_path = self._local_results.analysis_paths.file_s3_data_all_accounts
        return _CombinedAccountsS3DataCsvToDf().get_df(file_path)


class AccountS3DataFactory:
    def __init__(self, account: str):
        self._account = account
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def to_csv_extract_s3_data(self):
        _AccountExtractor(
            self._local_results.get_file_path_account_results(self._account),
            self._s3_uris_file_reader.get_s3_queries_for_account(self._account),
        ).extract()

    def get_df_from_csv(self) -> Df:
        return _AccountS3DataDfBuilder(self._account).with_multi_index().build()

    def get_df_from_csv_with_original_account_index(self) -> Df:
        return _AccountS3DataDfBuilder(self._account).with_multi_index().with_origin_account_index().build()


class _AccountExtractor:
    def __init__(self, file_path_results: Path, s3_queries: list[S3Query]):
        self._file_path_results = file_path_results
        self._logger = get_logger()
        self._s3_queries = s3_queries

    def extract(self):
        self._logger.info(f"Exporting AWS account information to {self._file_path_results}")
        for query_index, s3_query in enumerate(self._s3_queries, 1):
            self._logger.info(f"Analyzing S3 URI {query_index}/{len(self._s3_queries)}: {s3_query}")
            try:
                self._extract_s3_data_of_query(s3_query)
            except Exception as exception:
                self._drop_file()
                raise exception

    def _extract_s3_data_of_query(self, s3_query: S3Query):
        is_any_result = False
        for s3_data in S3Client(s3_query).get_s3_data():
            is_any_result = True
            self._export_s3_data_to_csv(s3_data, s3_query)
        if not is_any_result:
            self._export_s3_data_to_csv([FileS3Data()], s3_query)

    def _export_s3_data_to_csv(self, s3_data: S3Data, s3_query: S3Query):
        query_and_data_df = self._get_df_query_and_data(s3_data, s3_query)
        export_headers = not self._file_path_results.is_file()
        query_and_data_df.to_csv(header=export_headers, index=False, mode="a", path_or_buf=self._file_path_results)

    def _get_df_query_and_data(self, s3_data: S3Data, s3_query: S3Query) -> Df:
        result = Df(file_data._asdict() for file_data in s3_data)
        result.insert(0, "bucket", s3_query.bucket)
        result.insert(1, "prefix", s3_query.prefix)
        return result

    def _drop_file(self):
        self._file_path_results.unlink()


class _CombinedAccountsS3DataDfToCsv:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()
        self._logger = get_logger()

    def export(self, df: Df):
        file_path = LocalResults().analysis_paths.file_s3_data_all_accounts
        self._logger = get_logger()
        self._logger.info(f"Exporting all AWS accounts S3 files information to {file_path}")
        csv_df = self._get_df_to_export(df)
        csv_df.to_csv(file_path)

    def _get_df_to_export(self, df: Df) -> Df:
        result = df.copy()
        csv_column_names = ["_".join(values) for values in result.columns]
        csv_column_names = [
            self._get_csv_column_name_drop_undesired_text(column_name) for column_name in csv_column_names
        ]
        result.columns = csv_column_names
        account_1 = self._s3_uris_file_reader.get_first_account()
        result.index.names = [
            f"bucket_{account_1}",
            f"file_path_in_s3_{account_1}",
            "file_name_all_accounts",
        ]
        return result

    def _get_csv_column_name_drop_undesired_text(self, column_name: str) -> str:
        if column_name.startswith("analysis_"):
            return column_name.replace("analysis_", "", 1)
        return column_name


class _CombinedAccountsS3DataCsvToDf:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self, file_path_s3_data_all_accounts: Path) -> AllAccountsS3DataDf:
        result = self._get_df_from_file(file_path_s3_data_all_accounts)
        return self._get_df_set_multi_index_columns(result)

    # TODO extract common code with _get_df_account_from_file
    def _get_df_from_file(self, file_path_name: Path) -> Df:
        accounts = self._s3_uris_file_reader.get_accounts()
        return read_csv(
            file_path_name,
            index_col=[f"bucket_{accounts[0]}", f"file_path_in_s3_{accounts[0]}", "file_name_all_accounts"],
            parse_dates=[f"{account}_date" for account in accounts],
        ).astype({f"{account}_size": "Int64" for account in accounts})

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


class _AccountsS3DataDfCombinator:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> AllAccountsS3DataDf:
        result = self._get_df_combine_accounts_results()
        return self._get_df_drop_incorrect_empty_rows(result)

    def _get_df_combine_accounts_results(self) -> AllAccountsS3DataDf:
        accounts = self._s3_uris_file_reader.get_accounts()
        result = AccountS3DataFactory(accounts[0]).get_df_from_csv()
        for account in accounts[1:]:
            account_df = AccountS3DataFactory(account).get_df_from_csv_with_original_account_index()
            result = result.join(account_df, how="outer")
        return result

    def _get_df_drop_incorrect_empty_rows(self, df: AllAccountsS3DataDf) -> AllAccountsS3DataDf:
        """
        Drop null rows caused when merging query results without files in some accounts.
        Avoid drop queries without results in any aws account.
        """
        result = df
        count_files_per_bucket_and_path_df = (
            Df(result.index.to_list(), columns=result.index.names).groupby(["bucket", "prefix"]).count()
        )
        count_files_per_bucket_and_path_df.columns = MultiIndex.from_tuples(
            [
                ("count", "files_in_bucket_prefix"),
            ]
        )
        result = result.join(count_files_per_bucket_and_path_df)
        result = result.reset_index()
        result = result.loc[(~result["name"].isna()) | (result[("count", "files_in_bucket_prefix")] == 0)]
        result = result.set_index(["bucket", "prefix", "name"])
        return result.drop(columns=(("count", "files_in_bucket_prefix")))


class _AccountS3DataDfBuilder:
    def __init__(self, account: str):
        self._account = account
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()
        self.__df = None  # To avoid read file more than once.

    def with_multi_index(self) -> "_AccountS3DataDfBuilder":
        self._df.columns = MultiIndex.from_tuples(self._column_names_mult_index)
        return self

    def with_origin_account_index(self) -> "_AccountS3DataDfBuilder":
        """The with_multi_index method must be called before this method is called"""
        self._df = _S3UriDfModifier(
            self._s3_uris_file_reader.get_first_account(), self._account, self._df
        ).get_df_set_s3_uris_in_origin_account()
        return self

    def build(self) -> AccountS3DataDf:
        return self._df

    @property
    def _df(self) -> Df:
        if self.__df is None:
            local_file_path_name = self._local_results.get_file_path_account_results(self._account)
            self.__df = self._get_df_account_from_file(local_file_path_name)
        return self.__df

    def _get_df_account_from_file(self, file_path_name: Path) -> Df:
        return pd.read_csv(
            file_path_name,
            index_col=["bucket", "prefix", "name"],
            parse_dates=["date"],
        ).astype({"size": "Int64"})

    @_df.setter
    def _df(self, df: Df):
        self.__df = df

    @property
    def _column_names_mult_index(self) -> list[tuple[str, str]]:
        return [(self._account, column_name) for column_name in self._df.columns]


class _S3UriDfModifier:
    def __init__(self, *args):
        self._account_origin, self._account_target, self._df = args
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self) -> Df:
        s3_uris_map_df = self._s3_uris_file_reader.get_df_s3_uris_map_between_accounts(
            self._account_origin, self._account_target
        )
        return self._get_df_set_s3_uris_in_origin_account(s3_uris_map_df)

    # TODO rename, it has the same name as the public method
    # TODO refactor, too long
    def _get_df_set_s3_uris_in_origin_account(self, s3_uris_map_df: Df) -> Df:
        if s3_uris_map_df[self._account_origin].equals(s3_uris_map_df[self._account_target]):
            return self._df
        original_length = len(self._df)
        assert self._df.index.get_level_values("prefix").str.endswith("/").all()
        result = self._df.copy()
        result = result.reset_index("name")
        s3_uris_map_df = self._get_s3_uris_map_prepared_for_join(s3_uris_map_df)
        result = result.join(s3_uris_map_df)
        if True in result[[f"{self._account_origin}_bucket", f"{self._account_origin}_prefix"]].isna().any().values:
            raise ValueError("Some values could not be replaced")
        result = result.reset_index(drop=True)
        result = result.rename(
            columns={f"{self._account_origin}_bucket": "bucket", f"{self._account_origin}_prefix": "prefix"}
        )
        result = result.set_index(["bucket", "prefix", "name"])
        assert original_length == len(result)
        return result

    def _get_s3_uris_map_prepared_for_join(self, s3_uris_map_df: Df) -> Df:
        result = s3_uris_map_df.copy()
        for account in (self._account_origin, self._account_target):
            result[[f"{account}_bucket", f"{account}_prefix"]] = result[account].str.extract(
                r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)", expand=False
            )
            result.loc[~result[f"{account}_prefix"].str.endswith("/"), f"{account}_prefix"] = (
                result[f"{account}_prefix"] + "/"
            )
        result.drop(columns=[f"{account}" for account in (self._account_origin, self._account_target)], inplace=True)
        result = result.rename(
            columns={f"{self._account_target}_bucket": "bucket", f"{self._account_target}_prefix": "prefix"}
        )
        result = result.set_index(["bucket", "prefix"])
        result.columns = [
            result.columns,
            [""] * len(result.columns),
        ]  # To merge to a MultiIndex columns Df.
        return result
