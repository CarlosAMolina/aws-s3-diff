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
        result = _AwsAccountS3DataDfBuilder(aws_accounts[0]).with_multi_index().build()
        for aws_account in aws_accounts[1:]:
            account_df = _AwsAccountS3DataDfBuilder(aws_account).with_multi_index().with_origin_account_index().build()
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


class _AwsAccountS3DataDfBuilder:
    def __init__(self, aws_account: str):
        self._aws_account = aws_account
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()
        self.__aws_accounts = None  # To avoid read file more than once.
        self.__df = None  # To avoid read file more than once.

    def with_multi_index(self) -> "_AwsAccountS3DataDfBuilder":
        self._df.columns = MultiIndex.from_tuples(self._column_names_mult_index)
        return self

    def with_origin_account_index(self) -> "_AwsAccountS3DataDfBuilder":
        """The with_multi_index method must be called before this method is called"""
        self._df = _S3UriDfModifier(
            self._aws_account_origin, self._aws_account, self._df
        ).get_df_set_s3_uris_in_origin_account()
        return self

    def build(self) -> AccountS3DataDf:
        return self._df

    @property
    def _df(self) -> Df:
        if self.__df is None:
            local_file_path_name = self._local_results.get_file_path_aws_account_results(self._aws_account)
            self.__df = self._get_df_aws_account_from_file(local_file_path_name)
        return self.__df

    def _get_df_aws_account_from_file(self, file_path_name: Path) -> Df:
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
        return [(self._aws_account, column_name) for column_name in self._df.columns]

    @property
    def _aws_account_origin(self) -> str:
        return self._aws_accounts[0]

    @property
    def _aws_accounts(self) -> list[str]:
        if self.__aws_accounts is None:
            self.__aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        return self.__aws_accounts


class _S3UriDfModifier:
    def __init__(self, *args):
        self._aws_account_origin, self._aws_account_target, self._df = args
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self) -> Df:
        s3_uris_map_df = self._s3_uris_file_reader.get_df_s3_uris_map_between_accounts(
            self._aws_account_origin, self._aws_account_target
        )
        return self._get_df_set_s3_uris_in_origin_account(s3_uris_map_df)

    # TODO rename, it has the same name as the public method
    # TODO refactor, too long
    def _get_df_set_s3_uris_in_origin_account(self, s3_uris_map_df: Df) -> Df:
        original_lenght = len(self._df)
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
        result.drop(["bucket_and_prefix", "current_value"], axis="columns", level=0, inplace=True)
        result = result.rename(columns={"new_value": "bucket_and_prefix"})
        # TODO use regex defined in config_files.py
        result[["bucket_new", "prefix_new"]] = result["bucket_and_prefix"].str.extract(
            r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)", expand=False
        )
        result.drop(["bucket", "prefix", "bucket_and_prefix"], axis="columns", level=0, inplace=True)
        result = result.rename(columns={"bucket_new": "bucket", "prefix_new": "prefix"})
        result.insert(0, "bucket", result.pop("bucket"))
        result.insert(1, "prefix", result.pop("prefix"))
        result["prefix"] = result["prefix"] + "/"
        result = result.set_index(["bucket", "prefix", "name"])
        final_length = len(result)
        assert original_lenght == final_length
        return result
