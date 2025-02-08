import re
from abc import ABC
from abc import abstractmethod
from collections import namedtuple

from pandas import DataFrame as Df
from pandas import Series

from config_files import AnalysisConfigReader
from local_results import LocalResults
from logger import get_logger
from s3_data.all_accounts import AllAccountsS3DataFactory
from s3_data.all_accounts import S3DataTransformer
from types_custom import MultiIndexDf


class AnalysisS3DataFactory:
    def __init__(self):
        self._all_accounts_s3_data_factory = AllAccountsS3DataFactory()
        self._analysis_config_reader = AnalysisConfigReader()
        self._analysis_as_single_index_factory = _AnalysisAsSingleIndexFactory()
        self._local_results = LocalResults()
        self._logger = get_logger()

    def to_csv(self):
        file_path = self._local_results.analysis_paths.file_analysis
        self._logger.info(f"Exporting analysis to {file_path}")
        df = self._get_df_s3_data_analyzed()
        csv_df = self._analysis_as_single_index_factory.get_df(df)
        csv_df.to_csv(file_path, index=False)

    def _get_df_s3_data_analyzed(self) -> MultiIndexDf:
        all_accounts_s3_data_df = self._all_accounts_s3_data_factory.get_df_from_csv()
        return self._get_df_set_analysis_columns(all_accounts_s3_data_df)

    def _get_df_set_analysis_columns(self, df: MultiIndexDf) -> Df:
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
        self._df = _FileCopiedTypeAnalysisFactory().get_df(self._df)
        return self

    def with_analysis_can_exist_files(self) -> "_AnalysisBuilder":
        self._df = _CanExistTypeAnalysisFactory().get_df(self._df)
        return self

    def build(self) -> Df:
        return self._df


_AccountsToCompare = namedtuple("_AccountsToCompare", "origin target")
_ArrayAccountsToCompare = list[_AccountsToCompare]
_ConditionConfig = dict[str, bool | str]


class _ArrayAccountsToCompareFactory(ABC):
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


class _FileCopiedAnalysisArrayAccountsToCompareFactory(_ArrayAccountsToCompareFactory):
    def _get_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_accounts_where_files_must_be_copied()


class _CanExistAnalysisArrayAccountsToCompareFactory(_ArrayAccountsToCompareFactory):
    def _get_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_accounts_that_must_not_have_more_files()


class _TypeAnalysisFactory(ABC):
    def get_df(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        for accounts in self._get_accounts_array():
            result = self._two_accounts_analysis_factory(accounts, result).get_df_set_analysis()
        return result

    @abstractmethod
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        pass

    # TODO use _TwoAccountsAnalysisFactory without " in return type
    @property
    @abstractmethod
    def _two_accounts_analysis_factory(self) -> type["_TwoAccountsAnalysisFactory"]:
        pass


class _FileCopiedTypeAnalysisFactory(_TypeAnalysisFactory):
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        return _FileCopiedAnalysisArrayAccountsToCompareFactory().get_array_accounts()

    @property
    def _two_accounts_analysis_factory(self) -> type["_TwoAccountsAnalysisFactory"]:
        return _IsFileCopiedTwoAccountsAnalysisFactory


class _CanExistTypeAnalysisFactory(_TypeAnalysisFactory):
    def _get_accounts_array(self) -> _ArrayAccountsToCompare:
        return _CanExistAnalysisArrayAccountsToCompareFactory().get_array_accounts()

    @property
    def _two_accounts_analysis_factory(self) -> type["_TwoAccountsAnalysisFactory"]:
        return _CanFileExistTwoAccountsAnalysisFactory


class _TwoAccountsAnalysisFactory(ABC):
    def __init__(self, accounts: _AccountsToCompare, df: MultiIndexDf):
        self._accounts = accounts
        self._condition = _AnalysisCondition(accounts, df)
        self._df = df
        self._logger = get_logger()

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


class _IsFileCopiedTwoAccountsAnalysisFactory(_TwoAccountsAnalysisFactory):
    def _log_analysis(self):
        self._logger.info(
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


class _CanFileExistTwoAccountsAnalysisFactory(_TwoAccountsAnalysisFactory):
    def _log_analysis(self):
        self._logger.info(
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


class _AnalysisAsSingleIndexFactory(S3DataTransformer):
    def get_df(self, df: MultiIndexDf) -> Df:
        return self.get_df_to_export(df)

    # TODO deprecate
    def get_df_to_export(self, df: MultiIndexDf) -> Df:
        result = df.copy()
        self._set_df_columns_as_single_index(result)
        result = result.rename(columns=lambda x: re.sub("^analysis_", "", x))
        return result.reset_index()
