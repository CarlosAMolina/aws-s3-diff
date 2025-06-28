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

logger = get_logger()


class AnalysisCsvExporter(CsvExporter):
    def __init__(self):
        self._analysis_csv_generator = AnalysisDataGenerator()
        self._local_results = LocalResults()

    def export_df(self, df: Df):
        file_path = self._local_results.get_file_path_analysis()
        logger.info(f"Exporting {file_path}")
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
        result_builder = _AnalysisBuilder(df)
        if len(self._analysis_config_reader.get_accounts_where_files_must_be_copied()):
            result_builder.with_analysis_is_file_copied()
        if len(self._analysis_config_reader.get_accounts_that_must_not_have_more_files()):
            result_builder.with_analysis_can_exist_files()
        return result_builder.build()

    def _get_df_with_single_index(self, df: Df) -> Df:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        result = result.rename(columns=lambda column_name: re.sub("^analysis_", "", column_name))
        return result.reset_index()

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)


class _AnalysisBuilder:
    def __init__(self, df: MultiIndexDf):
        self._df = df
        self._analysis_config_reader = AnalysisConfigReader()

    def with_analysis_is_file_copied(self) -> "_AnalysisBuilder":
        account_targets = self._analysis_config_reader.get_accounts_where_files_must_be_copied()
        self._df = _AllAccountsAnalysisSetter(
            account_targets, _IsFileCopiedTwoAccountsAnalysisSetter
        ).get_df_set_analysis_columns(self._df)
        return self

    def with_analysis_can_exist_files(self) -> "_AnalysisBuilder":
        account_targets = self._analysis_config_reader.get_accounts_that_must_not_have_more_files()
        self._df = _AllAccountsAnalysisSetter(
            account_targets, _CanFileExistTwoAccountsAnalysisSetter
        ).get_df_set_analysis_columns(self._df)
        return self

    def build(self) -> Df:
        return self._df


_AccountsToCompare = namedtuple("_AccountsToCompare", "origin target")
_ConditionConfig = dict[str, bool | str]


class _TwoAccountsAnalysisSetter(ABC):
    def __init__(self, accounts: _AccountsToCompare, df: MultiIndexDf):
        self._accounts = accounts
        self._condition = _AnalysisCondition(accounts, df)
        self._df = df

    @abstractmethod
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        pass

    def _get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[[("analysis", self._column_name_result)]] = None
        for (
            condition_name,
            condition_result_to_set,
        ) in self._condition_config.items():
            condition_results: Series = getattr(self._condition, condition_name)
            result.loc[condition_results, [("analysis", self._column_name_result)]] = condition_result_to_set
        return result

    @property
    @abstractmethod
    def _column_name_result(self) -> str:
        pass

    @property
    @abstractmethod
    def _condition_config(self) -> _ConditionConfig:
        pass


class _IsFileCopiedTwoAccountsAnalysisSetter(_TwoAccountsAnalysisSetter):
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        self._log_analysis()
        return self._get_df_set_analysis_columns(self._df.copy())

    def _log_analysis(self):
        logger.info(
            f"Analyzing if files of the account '{self._accounts.origin}' have been copied to the account"
            f" '{self._accounts.target}'"
        )

    @property
    def _column_name_result(self) -> str:
        return f"is_sync_ok_in_{self._accounts.target}"

    @property
    def _condition_config(self) -> _ConditionConfig:
        return {
            "condition_sync_is_wrong": False,
            "condition_sync_is_ok": True,
            "condition_no_file_at_origin_but_at_target": False,
            "condition_no_file_at_origin_or_target": True,
        }


class _CanFileExistTwoAccountsAnalysisSetter(_TwoAccountsAnalysisSetter):
    def get_df_set_analysis_column(self) -> MultiIndexDf:
        self._log_analysis()
        return self._get_df_set_analysis_columns(self._df.copy())

    def _log_analysis(self):
        logger.info(
            f"Analyzing if files in account '{self._accounts.target}' can exist, compared to account"
            f" '{self._accounts.origin}'"
        )

    @property
    def _column_name_result(self) -> str:
        return f"can_exist_in_{self._accounts.target}"

    @property
    def _condition_config(self) -> _ConditionConfig:
        return {"condition_must_not_exist": False}

    def _get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df
        result[[("analysis", self._column_name_result)]] = None
        condition_results: Series = self._condition.condition_must_not_exist
        result.loc[condition_results, [("analysis", self._column_name_result)]] = False
        return result


class _AllAccountsAnalysisSetter:
    def __init__(
        self, account_targets: list[str], two_accounts_analysis_creator_class: type[_TwoAccountsAnalysisSetter]
    ):
        self._account_targets = account_targets
        self._two_accounts_analysis_creator_class = two_accounts_analysis_creator_class
        self._analysis_config_reader = AnalysisConfigReader()

    def get_df_set_analysis_columns(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        account_origin = self._analysis_config_reader.get_account_origin()
        for account_target in self._account_targets:
            accounts = _AccountsToCompare(account_origin, account_target)
            result = self._two_accounts_analysis_creator_class(accounts, result).get_df_set_analysis_column()
        return result


class _AnalysisCondition:
    def __init__(self, accounts: _AccountsToCompare, df: Df):
        self._accounts = accounts
        self._df = df

    @property
    def condition_sync_is_wrong(self) -> Series:
        return self._is_file_in_account_origin & ~self._is_file_copied

    @property
    def condition_sync_is_ok(self) -> Series:
        return self._is_file_in_account_origin & self._is_file_copied

    @property
    def condition_no_file_at_origin_but_at_target(self) -> Series:
        return ~self._is_file_in_account_origin & self._is_file_in_account_target

    @property
    def condition_no_file_at_origin_or_target(self) -> Series:
        return ~self._is_file_in_account_origin & ~self._is_file_in_account_target

    @property
    def condition_must_not_exist(self) -> Series:
        return ~self._is_file_in_account_origin & self._is_file_in_account_target

    @property
    def _is_file_copied(self) -> Series:
        # Replace nan results to avoid incorrect values due to equality compaisons between null values.
        # https://pandas.pydata.org/docs/user_guide/missing_data.html#filling-missing-data
        return (
            self._df.loc[:, (self._accounts.origin, "hash")]
            .eq(self._df.loc[:, (self._accounts.target, "hash")])
            .fillna(False)
        )

    @property
    def _is_file_in_account_origin(self) -> Series:
        return self._df.loc[:, (self._accounts.origin, "size")].notnull()

    @property
    def _is_file_in_account_target(self) -> Series:
        return self._df.loc[:, (self._accounts.target, "size")].notnull()
