import re
from abc import ABC
from abc import abstractmethod
from collections import namedtuple
from pathlib import Path

from pandas import DataFrame as Df
from pandas import Index
from pandas import MultiIndex
from pandas import read_csv
from pandas import Series

from combine import get_df_combine_files
from local_results import LocalResults
from s3_uris_to_analyze import S3UrisFileReader
from types_custom import AllAccoutsS3DataDf


class S3DataAnalyzer:
    def run(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        self._show_summary(s3_analyzed_df)
        _AnalysisDfToCsv().export(s3_analyzed_df)

    def _get_df_s3_data_analyzed(self) -> Df:
        s3_data_df = get_df_combine_files()  # TODO move to combine.py
        _export_s3_data_of_all_accounts(s3_data_df)  # TODO move to combine.py
        all_accounts_s3_data_df = self._get_df_all_accounts_s3_data()
        return self._get_df_set_analysis(all_accounts_s3_data_df)

    def _get_df_all_accounts_s3_data(self) -> AllAccoutsS3DataDf:
        return _CombineCsvToDf().get_df()

    def _get_df_set_analysis(self, df: AllAccoutsS3DataDf) -> Df:
        aws_accounts_analysis = _AnalysisAwsAccountsGenerator().get_aws_accounts()
        return _S3DataSetAnalysis(aws_accounts_analysis).get_df_set_analysis_columns(df)

    def _show_summary(self, df: Df):
        aws_accounts_summary = _AnalysisAwsAccountsGenerator().get_aws_accounts()
        _show_summary(aws_accounts_summary, df)


class _AnalysisAwsAccounts:
    def __init__(self, *args):
        (
            self.aws_account_origin,
            self.aws_account_that_must_not_have_more_files,
            self.aws_accounts_where_files_must_be_copied,
        ) = args


_CompareAwsAccounts = namedtuple("_CompareAwsAccounts", "origin target")
_ConditionConfig = dict[str, bool | str]


class _AwsAccountsGenerator(ABC):
    @abstractmethod
    def get_aws_accounts(self) -> _AnalysisAwsAccounts:
        pass

    def _get_aws_account_with_data_to_sync(self) -> str:
        return S3UrisFileReader().get_aws_accounts()[0]

    def _get_aws_accounts_where_files_must_be_copied(self) -> list[str]:
        result = S3UrisFileReader().get_aws_accounts()
        result.remove(self._get_aws_account_with_data_to_sync())
        return result


class _AnalysisAwsAccountsGenerator(_AwsAccountsGenerator):
    def get_aws_accounts(self) -> _AnalysisAwsAccounts:
        return _AnalysisAwsAccounts(
            self._get_aws_account_with_data_to_sync(),
            self._get_aws_account_that_must_not_have_more_files(),
            self._get_aws_accounts_where_files_must_be_copied(),
        )

    def _get_aws_account_that_must_not_have_more_files(self) -> str:
        return S3UrisFileReader().get_aws_accounts()[1]


class _S3DataSetAnalysis:
    def __init__(self, aws_accounts: _AnalysisAwsAccounts):
        self._aws_accounts = aws_accounts

    def get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_file_has_been_copied(result)
        return self._get_df_set_analysis_must_file_exist(result)

    def _get_df_set_analysis_file_has_been_copied(self, df: Df) -> Df:
        result = df
        for aws_account_target in self._aws_accounts.aws_accounts_where_files_must_be_copied:
            aws_accounts = _CompareAwsAccounts(self._aws_accounts.aws_account_origin, aws_account_target)
            result = _OriginFileSyncDfAnalysis(aws_accounts, result).get_df_set_analysis()
        return result

    def _get_df_set_analysis_must_file_exist(self, df: Df) -> Df:
        aws_accounts = _CompareAwsAccounts(
            self._aws_accounts.aws_account_origin, self._aws_accounts.aws_account_that_must_not_have_more_files
        )
        return _TargetAccountWithoutMoreFilesDfAnalysis(aws_accounts, df).get_df_set_analysis()


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


class _DfAnalysis:
    def __init__(self, aws_accounts: _CompareAwsAccounts, df: Df):
        self._aws_account_target = aws_accounts.target
        self._condition = _AnalysisCondition(aws_accounts, df)
        self._df = df

    def get_df_set_analysis(self) -> Df:
        result = self._df.copy()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[[self._result_column_multi_index]] = None
        for (
            condition_name,
            condition_result,
        ) in self._analysis_config.condition_config.items():
            result.loc[
                getattr(self._condition, condition_name),
                [self._result_column_multi_index],
            ] = condition_result
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
            "condition_not_exist_file_to_sync": "No file to sync",
        }


class _TargetAccountWithoutMoreFilesAnalysisConfig(_AnalysisConfig):
    @property
    def column_name_result(self) -> str:
        return f"must_exist_in_{self._aws_account_target}"

    @property
    def condition_config(self) -> _ConditionConfig:
        return {"condition_must_not_exist": False}


class _AnalysisCondition:
    def __init__(self, aws_accounts: _CompareAwsAccounts, df: Df):
        self._aws_accounts = aws_accounts
        self._df = df

    @property
    def condition_sync_is_wrong(self) -> Series:
        return self.condition_exists_file_to_sync & ~self._condition_file_is_sync

    @property
    def condition_sync_is_ok(self) -> Series:
        return self.condition_exists_file_to_sync & self._condition_file_is_sync

    @property
    def condition_must_not_exist(self) -> Series:
        return ~self.condition_exists_file_to_sync & self._condition_exists_file_in_target_aws_account

    @property
    def condition_exists_file_to_sync(self) -> Series:
        return self._df.loc[:, self._column_index_size_origin].notnull()

    @property
    def condition_not_exist_file_to_sync(self) -> Series:
        return ~self.condition_exists_file_to_sync

    @property
    def _condition_file_is_sync(self) -> Series:
        return self._df.loc[:, self._column_index_size_origin] == self._df.loc[:, self._column_index_size_target]

    @property
    def _condition_exists_file_in_target_aws_account(self) -> Series:
        return self._df.loc[:, self._column_index_size_target].notnull()

    @property
    def _column_index_size_origin(self) -> tuple:
        return (self._aws_accounts.origin, "size")

    @property
    def _column_index_size_target(self) -> tuple:
        return (self._aws_accounts.target, "size")


def _show_summary(aws_accounts: _AnalysisAwsAccounts, df: Df):
    for aws_account_to_compare in aws_accounts.aws_accounts_where_files_must_be_copied:
        column_name_compare_result = f"is_sync_ok_in_{aws_account_to_compare}"
        condition = (df.loc[:, (aws_accounts.aws_account_origin, "size")].notnull()) & (
            df.loc[:, ("analysis", column_name_compare_result)].eq(False)
        )
        result = df[condition]
        print(f"Files not copied in {aws_account_to_compare} ({len(result)}):")
        print(result)


def _export_s3_data_of_all_accounts(df: Df):
    _CombineDfToCsv().export(df)


# TODO move to combine.py
class _CombineDfToCsv:
    def export(self, df: Df):
        file_path = LocalResults().get_file_path_s3_data_all_accounts()
        print(f"Exporting all AWS accounts S3 files information to {file_path}")
        csv_df = self._get_df_to_export(df)
        csv_df.to_csv(file_path)

    def _get_df_to_export(self, df: Df) -> Df:
        result = df.copy()
        csv_column_names = ["_".join(values) for values in result.columns]
        csv_column_names = [
            self._get_csv_column_name_drop_undesired_text(column_name) for column_name in csv_column_names
        ]
        result.columns = csv_column_names
        aws_account_1 = S3UrisFileReader().get_aws_accounts()[0]
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


class _CombineCsvToDf:
    def get_df(self) -> AllAccoutsS3DataDf:
        file_path = LocalResults().get_file_path_s3_data_all_accounts()
        result = self._get_df_from_file(file_path)
        return self._get_df_set_multi_index_columns(result)

    # TODO extract common code with combine.py._get_df_aws_account_from_file
    # TODO use in all scripts `file_path_in_s3_` instead of `file_path_`
    def _get_df_from_file(self, file_path_name: Path) -> Df:
        aws_accounts = S3UrisFileReader().get_aws_accounts()
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
        for aws_account in S3UrisFileReader().get_aws_accounts():
            regex_result = re.match(rf"{aws_account}_(?P<key>.*)", column_name)
            if regex_result is not None:
                return aws_account, regex_result.group("key")
        raise ValueError(f"Not managed column name: {column_name}")


# TODO refactor extract common code with  _CombineCsvTo
class _AnalysisDfToCsv:
    def export(self, df: Df):
        file_path = LocalResults().get_file_path_analysis_result()
        csv_df = self._get_df_to_export(df)
        csv_df.to_csv(file_path)

    def _get_df_to_export(self, df: Df) -> Df:
        result = df.copy()
        csv_column_names = ["_".join(values) for values in result.columns]
        csv_column_names = [
            self._get_csv_column_name_drop_undesired_text(column_name) for column_name in csv_column_names
        ]
        result.columns = csv_column_names
        aws_account_1 = S3UrisFileReader().get_aws_accounts()[0]
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
