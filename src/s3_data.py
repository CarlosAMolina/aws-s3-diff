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
from types_custom import AllAccountsS3DataDf
from types_custom import FileS3Data
from types_custom import S3Data
from types_custom import S3Query


def export_s3_data_all_accounts_to_one_file():
    s3_data_df = _IndividualAccountsS3DataCsvFilesToDf().get_df()
    _CombinedAccountsS3DataDfToCsv().export(s3_data_df)


def get_df_s3_data_all_accounts() -> AllAccountsS3DataDf:
    file_path = LocalResults().analysis_paths.file_s3_data_all_accounts
    return _CombinedAccountsS3DataCsvToDf().get_df(file_path)


class AwsAccountExtractor:
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
        aws_account_1 = self._s3_uris_file_reader.get_first_aws_account()
        result.index.names = [
            f"bucket_{aws_account_1}",
            f"file_path_in_s3_{aws_account_1}",
            "file_name_all_aws_accounts",
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

    # TODO extract common code with _get_df_aws_account_from_file
    def _get_df_from_file(self, file_path_name: Path) -> Df:
        aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        return read_csv(
            file_path_name,
            index_col=[f"bucket_{aws_accounts[0]}", f"file_path_in_s3_{aws_accounts[0]}", "file_name_all_aws_accounts"],
            parse_dates=[f"{aws_account}_date" for aws_account in aws_accounts],
        ).astype({f"{aws_account}_size": "Int64" for aws_account in aws_accounts})

    def _get_df_set_multi_index_columns(self, df: Df) -> Df:
        result = df
        result.columns = MultiIndex.from_tuples(self._get_multi_index_tuples_for_df_columns(result.columns))
        return result

    def _get_multi_index_tuples_for_df_columns(self, columns: Index) -> list[tuple[str, str]]:
        return [self._get_multi_index_from_column_name(column_name) for column_name in columns]

    def _get_multi_index_from_column_name(self, column_name: str) -> tuple[str, str]:
        for aws_account in self._s3_uris_file_reader.get_aws_accounts():
            regex_result = re.match(rf"{aws_account}_(?P<key>.*)", column_name)
            if regex_result is not None:
                return aws_account, regex_result.group("key")
        raise ValueError(f"Not managed column name: {column_name}")


class _IndividualAccountsS3DataCsvFilesToDf:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> AllAccountsS3DataDf:
        result = self._get_df_combine_aws_accounts_results()
        return self._get_df_drop_incorrect_empty_rows(result)

    def _get_df_combine_aws_accounts_results(self) -> AllAccountsS3DataDf:
        aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        result = self._get_df_for_aws_account(aws_accounts[0])
        for aws_account in aws_accounts[1:]:
            account_df = self._get_df_for_aws_account(aws_account)
            account_df = _S3UriDfModifier(
                aws_accounts[0], aws_account, account_df
            ).get_df_set_s3_uris_in_origin_account()
            result = result.join(account_df, how="outer")
        return result

    def _get_df_for_aws_account(self, aws_account: str) -> AllAccountsS3DataDf:
        local_file_path_name = LocalResults().get_file_path_aws_account_results(aws_account)
        result = self._get_df_aws_account_from_file(local_file_path_name)
        result.columns = MultiIndex.from_tuples(self._get_column_names_mult_index(aws_account, list(result.columns)))
        return result

    def _get_column_names_mult_index(self, aws_account: str, column_names: list[str]) -> list[tuple[str, str]]:
        return [(aws_account, column_name) for column_name in column_names]

    def _get_df_aws_account_from_file(self, file_path_name: Path) -> Df:
        return pd.read_csv(
            file_path_name,
            index_col=["bucket", "prefix", "name"],
            parse_dates=["date"],
        ).astype({"size": "Int64"})

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


class _S3UriDfModifier:
    def __init__(self, *args):
        self._aws_account_origin, self._aws_account_target, self._df = args
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self) -> Df:
        original_lenght = len(self._df)
        s3_uris_map_df = self._s3_uris_file_reader.get_df_s3_uris_map_between_accounts(
            self._aws_account_origin, self._aws_account_target
        )
        result = self._df.copy()
        result = result.reset_index()
        result["bucket_and_prefix"] = "s3://" + result["bucket"] + "/" + result["prefix"].str.rstrip("/")
        for aws_account in (self._aws_account_origin, self._aws_account_target):
            s3_uris_map_df.loc[:, aws_account] = s3_uris_map_df[aws_account].str.rstrip("/")
        s3_uris_map_df = s3_uris_map_df.rename(
            columns={self._aws_account_origin: "new_value", self._aws_account_target: "current_value"}
        )
        s3_uris_map_df.columns = [
            s3_uris_map_df.columns,
            [""] * len(s3_uris_map_df.columns),
        ]  # To merge to a MultiIndex Df.
        result = result.merge(s3_uris_map_df, left_on="bucket_and_prefix", right_on="current_value", how="left")
        if result["new_value"].isnull().any():
            df = result.loc[result["new_value"].isnull(), ["current_value", "new_value"]]
            error_text = f"These values have not been replaced:\n{df.to_string(index=False)}"
            raise ValueError(error_text)
        result.drop(["bucket_and_prefix", "current_value"], axis="columns", inplace=True)
        result = result.rename(columns={"new_value": "bucket_and_prefix"})
        # TODO use regex defined in config_files.py
        result[["bucket_new", "prefix_new"]] = result["bucket_and_prefix"].str.extract(
            r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)", expand=False
        )
        result.drop(["bucket", "prefix", "bucket_and_prefix"], axis="columns", inplace=True)
        result = result.rename(columns={"bucket_new": "bucket", "prefix_new": "prefix"})
        result.insert(0, "bucket", result.pop("bucket"))
        result.insert(1, "prefix", result.pop("prefix"))
        result["prefix"] = result["prefix"] + "/"
        result = result.set_index(["bucket", "prefix", "name"])
        final_length = len(result)
        assert original_lenght == final_length
        return result
        # TODO remove below methods
        return self._get_df_modify_buckets_and_paths(s3_uris_map_df)

    def _get_df_modify_buckets_and_paths(self, s3_uris_map_df: Df) -> Df:
        result = self._df.copy()
        new_multi_index_as_tuples = self._get_new_multi_index_as_tuples(result.index.tolist(), s3_uris_map_df)
        result.index = MultiIndex.from_tuples(new_multi_index_as_tuples, names=result.index.names)
        return result

    def _get_new_multi_index_as_tuples(self, old_multi_index_as_tuples: list[tuple], s3_uris_map_df: Df) -> list[tuple]:
        return [
            self._get_new_multi_index_as_tuple(old_multi_index_as_tuple, s3_uris_map_df)
            for old_multi_index_as_tuple in old_multi_index_as_tuples
        ]

    def _get_new_multi_index_as_tuple(self, old_multi_index_as_tuple: tuple, s3_uris_map_df: Df) -> tuple:
        old_bucket, old_prefix, old_file_name = old_multi_index_as_tuple
        query_to_use = self._get_s3_query_to_use(S3Query(old_bucket, old_prefix), s3_uris_map_df)
        return (query_to_use.bucket, query_to_use.prefix, old_file_name)

    def _get_s3_query_to_use(self, old_s3_query: S3Query, s3_uris_map_df: Df) -> S3Query:
        s3_uri_to_use = self._get_s3_uri_to_use(old_s3_query, s3_uris_map_df)
        return self._s3_uris_file_reader.get_s3_query_from_s3_uri(s3_uri_to_use)

    def _get_s3_uri_to_use(self, old_s3_query: S3Query, s3_uris_map_df: Df) -> str:
        iloc_row_to_use = self._get_iloc_of_the_s3_uri_to_use(
            old_s3_query, s3_uris_map_df[self._aws_account_target].tolist()
        )
        return s3_uris_map_df.iloc[iloc_row_to_use][self._aws_account_origin]

    def _get_iloc_of_the_s3_uri_to_use(self, old_s3_query: S3Query, new_s3_uris: list[str]) -> int:
        for index, new_s3_uri in enumerate(new_s3_uris):
            if old_s3_query == self._s3_uris_file_reader.get_s3_query_from_s3_uri(new_s3_uri):
                return index
        raise ValueError(f"{old_s3_query} is not in {new_s3_uris}")
