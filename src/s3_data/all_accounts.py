import re
from abc import ABC
from abc import abstractmethod
from pathlib import Path

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv

from config_files import REGEX_BUCKET_PREFIX_FROM_S3_URI
from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data.one_account import AccountS3DataFactory
from types_custom import AllAccountsS3DataDf
from types_custom import SingleIndexAllAccountsS3DataDf


class AllAccountsS3DataFactory:
    def __init__(self):
        self._accounts_s3_data_merger = _IndividualAccountS3DataMerger()
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

    def get_df_from_csv(self) -> AllAccountsS3DataDf:
        return _AccountsS3DataCsvReader().get_df()

    def _get_df_merging_each_account_s3_data(self) -> AllAccountsS3DataDf:
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
    def get_df_to_export(self, df: AllAccountsS3DataDf) -> SingleIndexAllAccountsS3DataDf:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        self._set_df_index_column_names(result)
        return result


class _AccountsS3DataCsvReader:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df(self) -> AllAccountsS3DataDf:
        # TODO don't access a property of a property
        result = self._get_df_from_file(self._local_results.analysis_paths.file_s3_data_all_accounts)
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


class _IndividualAccountS3DataMerger:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_merge_each_account_results(self) -> AllAccountsS3DataDf:
        return self._get_df_combine_accounts_results()
        # TODO RM return self._get_df_drop_incorrect_empty_rows(result)

    def _get_df_combine_accounts_results(self) -> AllAccountsS3DataDf:
        accounts = self._s3_uris_file_reader.get_accounts()
        result = self._s3_uris_file_reader.file_df[accounts[0]]
        # TODO refactor extract function, this lines is done in other files.
        result = result.str.extract(REGEX_BUCKET_PREFIX_FROM_S3_URI, expand=False)
        result.columns = ["bucket", "prefix"]
        # TODO refactor extract function, this lines is done in other files.
        result.loc[~result["prefix"].str.endswith("/"), "prefix"] = result["prefix"] + "/"
        result = result.set_index(result.columns.tolist())
        # TODO refactor extract function, this lines is done in other files.
        # TODO improve this, use MultiIndex or something better
        result.columns = [
            result.columns,
            [""] * len(result.columns),
        ]  # To merge to a MultiIndex columns Df.
        result_base = result.copy()
        account_df = AccountS3DataFactory(accounts[0]).get_df_from_csv()
        account_df = account_df.reset_index().set_index(result.index.names)
        results = list()
        results += [result_base.join(account_df).reset_index().set_index(["bucket", "prefix", "name"])]
        for account in accounts[1:]:
            account_df = (
                AccountS3DataFactory(account)
                .get_df_from_csv_with_original_account_index()
                .reset_index()
                .set_index(["bucket", "prefix"])
            )
            results += [result_base.join(account_df).reset_index().set_index(["bucket", "prefix", "name"])]
        result = results[0].join(results[1:])
        return result

    # TODO deprecate
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
