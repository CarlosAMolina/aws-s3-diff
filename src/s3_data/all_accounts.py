import re
from pathlib import Path

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data.one_account import AccountS3DataFactory
from types_custom import AllAccountsS3DataDf


class AllAccountsS3DataFactory:
    def __init__(self):
        self._accounts_s3_data_combinator = _AccountsS3DataCombinator()
        self._export_to_csv = _CombinedAccountsS3DataDfToCsv().export
        self._local_results = LocalResults()

    def to_csv(self):
        df = self._get_df_combine_accounts_s3_data()
        self._export_to_csv(df)

    def _get_df_combine_accounts_s3_data(self) -> AllAccountsS3DataDf:
        return self._accounts_s3_data_combinator.get_df()

    def get_df_from_csv(self) -> AllAccountsS3DataDf:
        # TODO don't access a property of a property
        file_path = self._local_results.analysis_paths.file_s3_data_all_accounts
        return _CombinedAccountsS3DataCsvToDf().get_df(file_path)


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


class _AccountsS3DataCombinator:
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
