from abc import ABC
from abc import abstractmethod
from collections import namedtuple
from typing import NamedTuple

from pandas import DataFrame as Df
from pandas import Series

from config_files import AnalysisConfigReader
from config_files import S3UrisFileReader
from local_results import LocalResults
from logger import get_logger
from s3_data import get_df_s3_data_all_accounts
from types_custom import AllAccoutsS3DataDf
from types_custom import AnalysisS3DataDf


class AnalysisGenerator:
    def export_analysis_file(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        self._export_analyzed_df_to_file(s3_analyzed_df)

    def _get_df_s3_data_analyzed(self) -> AnalysisS3DataDf:
        all_accounts_s3_data_df = get_df_s3_data_all_accounts()
        return _S3DataSetAnalysis().get_df_set_analysis_columns(all_accounts_s3_data_df)

    def _export_analyzed_df_to_file(self, df: AnalysisS3DataDf):
        _AnalysisDfToCsv().export(df)


_CompareAwsAccounts = namedtuple("_CompareAwsAccounts", "origin target")
_ConditionConfig = dict[str, bool | str]


class _ArrayCompareAwsAccountsGenerator:
    def __init__(self):
        self._analysis_config_reader = AnalysisConfigReader()

    def get_array_aws_accounts_to_analyze_if_files_have_been_copied(self) -> list[_CompareAwsAccounts]:
        return self._get_array_aws_accounts_for_target_accounts(
            self._analysis_config_reader.get_aws_accounts_where_files_must_be_copied()
        )

    def get_array_aws_accounts_to_analyze_account_without_more_files(self) -> list[_CompareAwsAccounts]:
        return self._get_array_aws_accounts_for_target_accounts(
            self._analysis_config_reader.get_aws_accounts_that_must_not_have_more_files()
        )

    def _get_array_aws_accounts_for_target_accounts(self, aws_account_targets: list[str]) -> list[_CompareAwsAccounts]:
        aws_account_origin = self._analysis_config_reader.get_aws_account_origin()
        return [
            _CompareAwsAccounts(aws_account_origin, aws_account_target) for aws_account_target in aws_account_targets
        ]


class _AnalysisSetterConfig(NamedTuple):
    df_analyzer: type["_DfAnalysis"]
    aws_accounts_array: list[_CompareAwsAccounts]
    log_message: str


# TODO rename all SetAnalysis to AnalysisSetter
class _S3DataSetAnalysis:
    def __init__(self):
        self._aws_accounts_generator = _ArrayCompareAwsAccountsGenerator()
        self._logger = get_logger()

    def get_df_set_analysis_columns(self, df: AllAccoutsS3DataDf) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_file_has_been_copied(result)
        return self._get_df_set_analysis_can_file_exist(result)

    def _get_df_set_analysis_file_has_been_copied(self, df: AllAccoutsS3DataDf) -> Df:
        config = _AnalysisSetterConfig(
            _OriginFileSyncDfAnalysis,
            self._aws_accounts_generator.get_array_aws_accounts_to_analyze_if_files_have_been_copied(),
            "Analyzing if files of the account '{origin}' have been copied to the account {target}",
        )
        return self._get_df_set_analysis(config, df)

    def _get_df_set_analysis_can_file_exist(self, df: AllAccoutsS3DataDf) -> Df:
        config = _AnalysisSetterConfig(
            _TargetAccountWithoutMoreFilesDfAnalysis,
            self._aws_accounts_generator.get_array_aws_accounts_to_analyze_account_without_more_files(),
            "Analyzing if the files of the account '{origin}' should exist in the account '{target}'",
        )
        return self._get_df_set_analysis(config, df)

    def _get_df_set_analysis(
        self,
        config: _AnalysisSetterConfig,
        df: AllAccoutsS3DataDf,
    ) -> Df:
        result = df.copy()
        for aws_accounts in config.aws_accounts_array:
            self._logger.info(config.log_message.format(origin=aws_accounts.origin, target=aws_accounts.target))
            result = config.df_analyzer(aws_accounts, result).get_df_set_analysis()
        return result


class _AnalysisConfig(ABC):
    def __init__(self, aws_account_target: str):
        self._aws_account_target = aws_account_target

    @property
    @abstractmethod
    def column_name_result(self) -> str:
        pass

    @property
    @abstractmethod
    def condition_config(self) -> _ConditionConfig:
        pass


# TODO refactor to ..Analyzer
class _DfAnalysis:
    def __init__(self, aws_accounts: _CompareAwsAccounts, df: Df):
        self._aws_account_target = aws_accounts.target
        self._condition = _AnalysisCondition(aws_accounts, df)
        self._df = df

    def get_df_set_analysis(self) -> AnalysisS3DataDf:
        result = self._df.copy()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[[self._result_column_multi_index]] = None
        for (
            condition_name,
            condition_result_to_set,
        ) in self._analysis_config.condition_config.items():
            condition_results: Series = getattr(self._condition, condition_name)
            result.loc[condition_results, [self._result_column_multi_index]] = condition_result_to_set
        return result

    @property
    def _result_column_multi_index(self) -> tuple[str, str]:
        return ("analysis", self._analysis_config.column_name_result)

    @property
    @abstractmethod
    def _analysis_config(self) -> _AnalysisConfig:
        pass


class _OriginFileSyncDfAnalysis(_DfAnalysis):
    @property
    def _analysis_config(self) -> _AnalysisConfig:
        return _OriginFileSyncAnalysisConfig(self._aws_account_target)


class _TargetAccountWithoutMoreFilesDfAnalysis(_DfAnalysis):
    @property
    def _analysis_config(self) -> _AnalysisConfig:
        return _TargetAccountWithoutMoreFilesAnalysisConfig(self._aws_account_target)


class _OriginFileSyncAnalysisConfig(_AnalysisConfig):
    @property
    def column_name_result(self) -> str:
        return f"is_sync_ok_in_{self._aws_account_target}"

    @property
    def condition_config(self) -> _ConditionConfig:
        return {
            "condition_sync_is_wrong": False,
            "condition_sync_is_ok": True,
            "condition_no_file_at_origin_but_at_target": False,
            "condition_no_file_at_origin_or_target": True,
        }


class _TargetAccountWithoutMoreFilesAnalysisConfig(_AnalysisConfig):
    @property
    def column_name_result(self) -> str:
        return f"can_exist_in_{self._aws_account_target}"

    @property
    def condition_config(self) -> _ConditionConfig:
        return {"condition_must_not_exist": False}


class _AnalysisCondition:
    def __init__(self, aws_accounts: _CompareAwsAccounts, df: Df):
        self._aws_accounts = aws_accounts
        self._df = df

    @property
    def condition_sync_is_wrong(self) -> Series:
        return self._condition_exists_file_to_sync & ~self._condition_file_is_sync

    @property
    def condition_sync_is_ok(self) -> Series:
        return self._condition_exists_file_to_sync & self._condition_file_is_sync

    @property
    def condition_no_file_at_origin_but_at_target(self) -> Series:
        return ~self._condition_exists_file_to_sync & self._condition_exists_file_in_target_aws_account

    @property
    def condition_no_file_at_origin_or_target(self) -> Series:
        return ~self._condition_exists_file_to_sync & ~self._condition_exists_file_in_target_aws_account

    @property
    def condition_must_not_exist(self) -> Series:
        return ~self._condition_exists_file_to_sync & self._condition_exists_file_in_target_aws_account

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
    def _condition_exists_file_in_target_aws_account(self) -> Series:
        return self._df.loc[:, self._column_index_size_target].notnull()

    @property
    def _column_index_size_origin(self) -> tuple:
        return self._get_column_index_size_for_account(self._aws_accounts.origin)

    @property
    def _column_index_size_target(self) -> tuple:
        return self._get_column_index_size_for_account(self._aws_accounts.target)

    def _get_column_index_size_for_account(self, aws_account: str) -> tuple:
        return (aws_account, "size")

    @property
    def _column_index_hash_origin(self) -> tuple:
        return self._get_column_index_hash_for_account(self._aws_accounts.origin)

    @property
    def _column_index_hash_target(self) -> tuple:
        return self._get_column_index_hash_for_account(self._aws_accounts.target)

    def _get_column_index_hash_for_account(self, aws_account: str) -> tuple:
        return (aws_account, "hash")


# TODO refactor extract common code with classes ..CsvToDf (in other files)
class _AnalysisDfToCsv:
    def __init__(self):
        self._logger = get_logger()
        self._s3_uris_file_reader = S3UrisFileReader()

    def export(self, df: AnalysisS3DataDf):
        file_path = LocalResults().analysis_paths.file_analysis
        csv_df = self._get_df_to_export(df)
        self._logger.info(f"Exporting analysis to {file_path}")
        csv_df.to_csv(file_path)

    def _get_df_to_export(self, df: AnalysisS3DataDf) -> Df:
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
