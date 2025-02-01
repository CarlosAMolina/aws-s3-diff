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
from s3_data import AllAccountsS3DataFactory
from types_custom import AllAccountsS3DataDf
from types_custom import AnalysisS3DataDf


class AnalysisS3DataFactory:
    def __init__(self):
        self._all_accounts_s3_data_factory = AllAccountsS3DataFactory()
        self._analysis_df_to_csv = _AnalysisDfToCsv()
        self._analysis_config_reader = AnalysisConfigReader()

    def to_csv(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        self._analysis_df_to_csv.export(s3_analyzed_df)

    def _get_df_s3_data_analyzed(self) -> AnalysisS3DataDf:
        all_accounts_s3_data_df = self._all_accounts_s3_data_factory.get_df_from_csv()
        return self._get_df_set_analysis_columns(all_accounts_s3_data_df)

    def _get_df_set_analysis_columns(self, df: AllAccountsS3DataDf) -> Df:
        result_builder = _AnalysisBuilder(df)
        if len(self._analysis_config_reader.get_aws_accounts_where_files_must_be_copied()):
            result_builder.with_analysis_is_file_copied()
        if len(self._analysis_config_reader.get_aws_accounts_that_must_not_have_more_files()):
            result_builder.with_analysis_no_more_files()
        return result_builder.build()


_AwsAccountsToCompare = namedtuple("_AwsAccountsToCompare", "origin target")
_ArrayAwsAccountsToCompare = list[_AwsAccountsToCompare]
_ConditionConfig = dict[str, bool | str]


class _ArrayAwsAccountsToCompareFactory(ABC):
    def __init__(self):
        self._analysis_config_reader = AnalysisConfigReader()

    def get_array_aws_accounts(self) -> _ArrayAwsAccountsToCompare:
        return self._get_array_aws_accounts_for_target_accounts(self._get_aws_account_targets())

    def _get_array_aws_accounts_for_target_accounts(self, aws_account_targets: list[str]) -> _ArrayAwsAccountsToCompare:
        aws_account_origin = self._analysis_config_reader.get_aws_account_origin()
        return [
            _AwsAccountsToCompare(aws_account_origin, aws_account_target) for aws_account_target in aws_account_targets
        ]

    @abstractmethod
    def _get_aws_account_targets(self) -> list[str]:
        pass


class _FileCopiedAnalysisArrayAwsAccountsToCompareFactory(_ArrayAwsAccountsToCompareFactory):
    def _get_aws_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_aws_accounts_where_files_must_be_copied()


class _NoMoreFilesAnalysisArrayAwsAccountsToCompareFactory(_ArrayAwsAccountsToCompareFactory):
    def _get_aws_account_targets(self) -> list[str]:
        return self._analysis_config_reader.get_aws_accounts_that_must_not_have_more_files()


class _AnalysisConfig(NamedTuple):
    analysis_factory: type["_AwsAccountsToCompareAnalysisFactory"]
    aws_accounts_array: _ArrayAwsAccountsToCompare


class _AnalysisConfigFactory(ABC):
    @abstractmethod
    def get_config(self) -> _AnalysisConfig:
        pass


class _FileCopiedAnalysisConfigFactory(_AnalysisConfigFactory):
    def get_config(self) -> _AnalysisConfig:
        return _AnalysisConfig(
            _IsFileCopiedAwsAccountsToCompareAnalysisFactory,
            _FileCopiedAnalysisArrayAwsAccountsToCompareFactory().get_array_aws_accounts(),
        )


class _NoMoreFilesAnalysisConfigFactory(_AnalysisConfigFactory):
    def get_config(self) -> _AnalysisConfig:
        return _AnalysisConfig(
            _CanFileExistAwsAccountsToCompareAnalysisFactory,
            _NoMoreFilesAnalysisArrayAwsAccountsToCompareFactory().get_array_aws_accounts(),
        )


class _AnalysisBuilder:
    def __init__(self, df: AllAccountsS3DataDf):
        self._df = df

    def with_analysis_is_file_copied(self) -> "_AnalysisBuilder":
        self._set_analysis(_FileCopiedAnalysisConfigFactory())
        return self

    def with_analysis_no_more_files(self) -> "_AnalysisBuilder":
        self._set_analysis(_NoMoreFilesAnalysisConfigFactory())
        return self

    def _set_analysis(self, config_factory: _AnalysisConfigFactory):
        config = config_factory.get_config()
        for aws_accounts in config.aws_accounts_array:
            self._df = config.analysis_factory(aws_accounts, self._df).get_df_set_analysis()

    def build(self) -> Df:
        return self._df


class _AwsAccountsToCompareAnalysisFactory(ABC):
    def __init__(self, aws_accounts: _AwsAccountsToCompare, df: AllAccountsS3DataDf):
        self._aws_accounts = aws_accounts
        self._condition = _AnalysisCondition(aws_accounts, df)
        self._df = df
        self._logger = get_logger()

    def get_df_set_analysis(self) -> AnalysisS3DataDf:
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


class _IsFileCopiedAwsAccountsToCompareAnalysisFactory(_AwsAccountsToCompareAnalysisFactory):
    def _log_analysis(self):
        self._logger.info(
            f"Analyzing if files of the account '{self._aws_accounts.origin}' have been copied to the account"
            f" '{self._aws_accounts.target}'"
        )

    @property
    def _column_name_result(self) -> str:
        return f"is_sync_ok_in_{self._aws_accounts.target}"

    @property
    def _condition_config(self) -> _ConditionConfig:
        return {
            "condition_sync_is_wrong": False,
            "condition_sync_is_ok": True,
            "condition_no_file_at_origin_but_at_target": False,
            "condition_no_file_at_origin_or_target": True,
        }


class _CanFileExistAwsAccountsToCompareAnalysisFactory(_AwsAccountsToCompareAnalysisFactory):
    def _log_analysis(self):
        self._logger.info(
            f"Analyzing if files in account '{self._aws_accounts.target}' can exist, compared to account"
            f" '{self._aws_accounts.origin}'"
        )

    @property
    def _column_name_result(self) -> str:
        return f"can_exist_in_{self._aws_accounts.target}"

    @property
    def _condition_config(self) -> _ConditionConfig:
        return {"condition_must_not_exist": False}


class _AnalysisCondition:
    def __init__(self, aws_accounts: _AwsAccountsToCompare, df: Df):
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
        self._local_results = LocalResults()

    def export(self, df: AnalysisS3DataDf):
        file_path = self._local_results.analysis_paths.file_analysis
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
