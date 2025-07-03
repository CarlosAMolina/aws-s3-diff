import re
from abc import ABC
from abc import abstractmethod
from collections import namedtuple

from pandas import DataFrame as Df
from pandas import Series

from aws_s3_diff.config_files import AnalysisConfigReader
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
from aws_s3_diff.s3_data.all_accounts import AccountsCsvReader
from aws_s3_diff.s3_data.interface import CsvExporter
from aws_s3_diff.s3_data.interface import DataGenerator
from aws_s3_diff.types_custom import MultiIndexDf

_logger = get_logger()


class AnalysisCsvExporter(CsvExporter):
    def __init__(self):
        self._local_results = LocalResults()

    def export_df(self, df: Df):
        file_path = self._local_results.get_file_path_analysis()
        _logger.info(f"Exporting {file_path}")
        df.to_csv(index=False, path_or_buf=file_path)


class AnalysisDataGenerator(DataGenerator):
    def __init__(self):
        self._accounts_csv_reader = AccountsCsvReader()
        self._analysis_config_reader = AnalysisConfigReader()

    def get_df(self) -> Df:
        df = self._get_df_s3_data_analyzed()
        return self._get_df_with_single_index(df)

    def _get_df_s3_data_analyzed(self) -> Df:
        all_accounts_s3_data_df = self._accounts_csv_reader.get_df()
        return self._get_df_set_analysis_columns(all_accounts_s3_data_df)

    def _get_df_set_analysis_columns(self, df: Df) -> Df:
        account_origin = self._analysis_config_reader.get_account_origin()
        result_builder = _AnalysisBuilder(account_origin, df)
        if len(self._analysis_config_reader.get_accounts_where_files_must_be_copied()):
            result_builder.with_analysis_is_file_copied()
        if len(self._analysis_config_reader.get_accounts_that_must_not_have_more_files()):
            result_builder.with_analysis_can_the_file_exist()
        return result_builder.build()

    def _get_df_with_single_index(self, df: Df) -> Df:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        result = result.rename(columns=lambda column_name: re.sub("^analysis_", "", column_name))
        return result.reset_index()

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)


_AccountsToCompare = namedtuple("_AccountsToCompare", "origin target")


class _TwoAccountsAnalysisSetter(ABC):
    def __init__(self, accounts: _AccountsToCompare, df: MultiIndexDf):
        self._accounts = accounts
        self._df = df

    @abstractmethod
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        pass

    @property
    def _is_the_same_file_in_both_accounts(self) -> Series:
        # Replace nan results to avoid incorrect values due to equality comparisons between null values.
        # https://pandas.pydata.org/docs/user_guide/missing_data.html#filling-missing-data
        return (
            self._df.loc[:, (self._accounts.origin, "hash")]
            .eq(self._df.loc[:, (self._accounts.target, "hash")])
            .fillna(False)
        )

    @property
    def _has_the_origin_account_a_file(self) -> Series:
        return self._df.loc[:, (self._accounts.origin, "size")].notnull()

    @property
    def _has_the_target_account_a_file(self) -> Series:
        return self._df.loc[:, (self._accounts.target, "size")].notnull()


class _IsFileCopiedTwoAccountsAnalysisSetter(_TwoAccountsAnalysisSetter):
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        _logger.info(
            f"Analyzing if files of the account '{self._accounts.origin}' have been copied to the account"
            f" '{self._accounts.target}'"
        )
        result = self._df.copy()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[[("analysis", self._column_name_result)]] = None
        result.loc[
            self._has_the_origin_account_a_file & ~self._is_the_same_file_in_both_accounts,
            [("analysis", self._column_name_result)],
        ] = False
        result.loc[
            self._has_the_origin_account_a_file & self._is_the_same_file_in_both_accounts,
            [("analysis", self._column_name_result)],
        ] = True
        result.loc[
            ~self._has_the_origin_account_a_file & self._has_the_target_account_a_file,
            [("analysis", self._column_name_result)],
        ] = False
        result.loc[
            ~self._has_the_origin_account_a_file & ~self._has_the_target_account_a_file,
            [("analysis", self._column_name_result)],
        ] = True
        return result

    @property
    def _column_name_result(self) -> str:
        return f"is_sync_ok_in_{self._accounts.target}"


class _CanFileExistTwoAccountsAnalysisSetter(_TwoAccountsAnalysisSetter):
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        _logger.info(
            f"Analyzing if files in account '{self._accounts.target}' can exist, compared to account"
            f" '{self._accounts.origin}'"
        )
        result = self._df.copy()
        result[[("analysis", self._column_name_result)]] = None
        result.loc[
            ~self._has_the_origin_account_a_file & self._has_the_target_account_a_file,
            [("analysis", self._column_name_result)],
        ] = False
        return result

    @property
    def _column_name_result(self) -> str:
        return f"can_exist_in_{self._accounts.target}"


class _AnalysisBuilder:
    def __init__(self, account_origin: str, df: MultiIndexDf):
        self._account_origin = account_origin
        self._df = df
        self._analysis_config_reader = AnalysisConfigReader()

    def with_analysis_is_file_copied(self) -> "_AnalysisBuilder":
        account_targets = self._analysis_config_reader.get_accounts_where_files_must_be_copied()
        self._set_analysis_columns_for_all_accounts(account_targets, _IsFileCopiedTwoAccountsAnalysisSetter)
        return self

    def with_analysis_can_the_file_exist(self) -> "_AnalysisBuilder":
        account_targets = self._analysis_config_reader.get_accounts_that_must_not_have_more_files()
        self._set_analysis_columns_for_all_accounts(account_targets, _CanFileExistTwoAccountsAnalysisSetter)
        return self

    def build(self) -> Df:
        return self._df

    def _set_analysis_columns_for_all_accounts(
        self,
        account_targets: list[str],
        two_accounts_analysis_setter_class: type[_TwoAccountsAnalysisSetter],
    ):
        for account_target in account_targets:
            accounts = _AccountsToCompare(self._account_origin, account_target)
            self._df = two_accounts_analysis_setter_class(accounts, self._df).get_df_set_analysis_column()
