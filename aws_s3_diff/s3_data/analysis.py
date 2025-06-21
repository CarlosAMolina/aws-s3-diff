import re
from abc import ABC
from abc import abstractmethod
from collections import namedtuple

from pandas import DataFrame as Df
from pandas import Series

from aws_s3_diff.config_files import AnalysisConfigReader
from aws_s3_diff.local_results import ANALYSIS_FILE_NAME
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
from aws_s3_diff.s3_data.all_accounts import AccountsCsvReader
from aws_s3_diff.s3_data.interface import CsvExporter
from aws_s3_diff.s3_data.interface import DataGenerator
from aws_s3_diff.s3_data.interface import FromMultiSimpleIndexDfCreator
from aws_s3_diff.s3_data.interface import MultiIndexDfCreator
from aws_s3_diff.s3_data.interface import NewDfCreator
from aws_s3_diff.types_custom import MultiIndexDf

logger = get_logger()


# TODO
class _AccountMultiIndexDfCreator(MultiIndexDfCreator):
    pass


class AnalysisCsvExporter(CsvExporter):
    def __init__(self):
        self._analysis_csv_generator = AnalysisDataGenerator()
        self._local_results = LocalResults()

    def export_df(self, df: Df):
        # TODO make private when the class AnalysisCsvExporter is created and use the
        # TODO method get_file_path_analysis
        file_path = self._local_results.get_file_path_results(ANALYSIS_FILE_NAME)
        logger.info(f"Exporting {file_path}")
        df.to_csv(index=False, path_or_buf=file_path)


class AnalysisDataGenerator(DataGenerator):
    def __init__(self):
        self._analysis_new_df_creator = _AnalysisNewDfCreator()

    def get_df(self) -> Df:
        df = self._analysis_new_df_creator.get_df()
        # TODO initialize in __init__
        return _AnalysisFromMultiSimpleIndexDfCreator(df).get_df()


class _AnalysisNewDfCreator(NewDfCreator):
    def __init__(self):
        self._accounts_csv_reader = AccountsCsvReader()
        self._analysis_config_reader = AnalysisConfigReader()

    def get_df(self) -> MultiIndexDf:
        return self._get_df_s3_data_analyzed()

    def _get_df_s3_data_analyzed(self) -> MultiIndexDf:
        all_accounts_s3_data_df = self._accounts_csv_reader.get_df()
        return self._get_df_set_analysis_columns(all_accounts_s3_data_df)

    def _get_df_set_analysis_columns(self, df: MultiIndexDf) -> MultiIndexDf:
        result_builder = _AnalysisBuilder(df)
        if len(self._analysis_config_reader.get_accounts_where_files_must_be_copied()):
            result_builder.with_analysis_is_file_copied()
        if len(self._analysis_config_reader.get_accounts_that_must_not_have_more_files()):
            result_builder.with_analysis_can_exist_files()
        return result_builder.build()


class _AnalysisBuilder:
    def __init__(self, df: MultiIndexDf):
        self._df = df

    def with_analysis_is_file_copied(self) -> "_AnalysisBuilder":
        self._df = _FileCopiedTypeAnalysisCreator().get_df(self._df)
        return self

    def with_analysis_can_exist_files(self) -> "_AnalysisBuilder":
        self._df = _CanExistTypeAnalysisCreator().get_df(self._df)
        return self

    def build(self) -> Df:
        return self._df


_AccountsToCompare = namedtuple("_AccountsToCompare", "origin target")
_ArrayAccountsToCompare = list[_AccountsToCompare]
_ConditionConfig = dict[str, bool | str]


class _ArrayAccountsToCompareCreator(ABC):
    def __init__(self):
        self._analysis_config_reader = AnalysisConfigReader()

    def get_array_accounts(self) -> _ArrayAccountsToCompare:
        return self._get_array_accounts_for_target_accounts(self._get_account_targets())

    def _get_array_accounts_for_target_accounts(self, account_targets: list[str]) -> _ArrayAccountsToCompare:
        account_origin = self._analysis_config_reader.get_account_origin()
        return [_AccountsToCompare(account_origin, account_target) for account_target in account_targets]

    @abstractmethod
    def _get_account_targets(self) -> list[str]:
        pass


class _FileCopiedAnalysisArrayAccountsToCompareCreator(_ArrayAccountsToCompareCreator):
    def _get_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_accounts_where_files_must_be_copied()


class _CanExistAnalysisArrayAccountsToCompareCreator(_ArrayAccountsToCompareCreator):
    def _get_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_accounts_that_must_not_have_more_files()


class _TypeAnalysisCreator(ABC):
    def get_df(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        for accounts in self._get_accounts_array():
            result = self._two_accounts_analysis_creator(accounts, result).get_df_set_analysis()
        return result

    @abstractmethod
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        pass

    # TODO use _TwoAccountsAnalysisCreator without " in return type
    @property
    @abstractmethod
    def _two_accounts_analysis_creator(self) -> type["_TwoAccountsAnalysisCreator"]:
        pass


class _FileCopiedTypeAnalysisCreator(_TypeAnalysisCreator):
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        return _FileCopiedAnalysisArrayAccountsToCompareCreator().get_array_accounts()

    @property
    def _two_accounts_analysis_creator(self) -> type["_TwoAccountsAnalysisCreator"]:
        return _IsFileCopiedTwoAccountsAnalysisCreator


class _CanExistTypeAnalysisCreator(_TypeAnalysisCreator):
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        return _CanExistAnalysisArrayAccountsToCompareCreator().get_array_accounts()

    @property
    def _two_accounts_analysis_creator(self) -> type["_TwoAccountsAnalysisCreator"]:
        return _CanFileExistTwoAccountsAnalysisCreator


class _TwoAccountsAnalysisCreator(ABC):
    def __init__(self, accounts: _AccountsToCompare, df: MultiIndexDf):
        self._accounts = accounts
        self._condition = _AnalysisCondition(accounts, df)
        self._df = df

    def get_df_set_analysis(self) -> MultiIndexDf:
        self._log_analysis()
        result = self._df.copy()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[[self._result_column_multi_index]] = None
        for (
            condition_name,
            condition_result_to_set,
        ) in self._condition_config.items():
            condition_results: Series = getattr(self._condition, condition_name)
            result.loc[condition_results, [self._result_column_multi_index]] = condition_result_to_set
        return result

    @abstractmethod
    def _log_analysis(self):
        pass

    @property
    def _result_column_multi_index(self) -> tuple[str, str]:
        return ("analysis", self._column_name_result)

    @property
    @abstractmethod
    def _column_name_result(self) -> str:
        pass

    @property
    @abstractmethod
    def _condition_config(self) -> _ConditionConfig:
        pass


class _IsFileCopiedTwoAccountsAnalysisCreator(_TwoAccountsAnalysisCreator):
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


class _CanFileExistTwoAccountsAnalysisCreator(_TwoAccountsAnalysisCreator):
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


class _AnalysisCondition:
    def __init__(self, accounts: _AccountsToCompare, df: Df):
        self._accounts = accounts
        self._df = df

    @property
    def condition_sync_is_wrong(self) -> Series:
        return self._condition_exists_file_to_sync & ~self._condition_file_is_sync

    @property
    def condition_sync_is_ok(self) -> Series:
        return self._condition_exists_file_to_sync & self._condition_file_is_sync

    @property
    def condition_no_file_at_origin_but_at_target(self) -> Series:
        return ~self._condition_exists_file_to_sync & self._condition_exists_file_in_target_account

    @property
    def condition_no_file_at_origin_or_target(self) -> Series:
        return ~self._condition_exists_file_to_sync & ~self._condition_exists_file_in_target_account

    @property
    def condition_must_not_exist(self) -> Series:
        return ~self._condition_exists_file_to_sync & self._condition_exists_file_in_target_account

    @property
    def condition_not_exist_file_to_sync(self) -> Series:
        return ~self._condition_exists_file_to_sync

    @property
    def _condition_exists_file_to_sync(self) -> Series:
        return self._df.loc[:, self._column_index_size_origin].notnull()

    @property
    def _condition_file_is_sync(self) -> Series:
        # Replace nan results to avoid incorrect values due to equality compaisons between null values.
        # https://pandas.pydata.org/docs/user_guide/missing_data.html#filling-missing-data
        return (
            self._df.loc[:, self._column_index_hash_origin]
            .eq(self._df.loc[:, self._column_index_hash_target])
            .fillna(False)
        )

    @property
    def _condition_exists_file_in_target_account(self) -> Series:
        return self._df.loc[:, self._column_index_size_target].notnull()

    @property
    def _column_index_size_origin(self) -> tuple:
        return self._get_column_index_size_for_account(self._accounts.origin)

    @property
    def _column_index_size_target(self) -> tuple:
        return self._get_column_index_size_for_account(self._accounts.target)

    def _get_column_index_size_for_account(self, account: str) -> tuple:
        return (account, "size")

    @property
    def _column_index_hash_origin(self) -> tuple:
        return self._get_column_index_hash_for_account(self._accounts.origin)

    @property
    def _column_index_hash_target(self) -> tuple:
        return self._get_column_index_hash_for_account(self._accounts.target)

    def _get_column_index_hash_for_account(self, account: str) -> tuple:
        return (account, "hash")


class _AnalysisFromMultiSimpleIndexDfCreator(FromMultiSimpleIndexDfCreator):
    def get_df(self) -> Df:
        result = self._df.copy()
        self._set_df_columns_as_single_index(result)
        result = result.rename(columns=lambda x: re.sub("^analysis_", "", x))
        return result.reset_index()
