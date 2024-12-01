from pandas import DataFrame as Df
from pandas import Series

from combine import get_df_combine_files
from local_results import LocalResults
from s3_uris_to_analyze import S3UrisFileReader


class S3DataAnalyzer:
    def run(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        _show_summary(
            self._get_aws_account_with_data_to_sync(), self._get_accounts_where_files_must_be_copied(), s3_analyzed_df
        )
        # TODO save in this projects instead of in /tmp
        _AnalysisDfToCsv().export(s3_analyzed_df, "/tmp/analysis.csv")

    def _get_df_s3_data_analyzed(self) -> Df:
        s3_data_df = get_df_combine_files()
        return _S3DataSetAnalysis(
            self._get_aws_account_with_data_to_sync(),
            self._get_aws_account_that_must_not_have_more_files(),
            self._get_accounts_where_files_must_be_copied(),
        ).get_df_set_analysis_columns(s3_data_df)

    def _get_aws_account_with_data_to_sync(self) -> str:
        return S3UrisFileReader().get_aws_accounts()[0]

    def _get_accounts_where_files_must_be_copied(self) -> list[str]:
        result = S3UrisFileReader().get_aws_accounts()
        result.remove(self._get_aws_account_with_data_to_sync())
        return result

    def _get_aws_account_that_must_not_have_more_files(self) -> str:
        # TODO use S3UrisFileReader instead of LocalResults
        for aws_account in LocalResults()._get_aws_accounts_analyzed():
            if aws_account.startswith("aws_account_2"):
                return aws_account
        raise ValueError("No aws account that must not have more files")


class _S3DataSetAnalysis:
    def __init__(
        self,
        aws_account_origin: str,
        aws_account_that_must_not_have_more_files: str,
        accounts_where_files_must_be_copied: list[str],
    ):
        self._aws_account_origin = aws_account_origin
        self._aws_account_that_must_not_have_more_files = aws_account_that_must_not_have_more_files
        self._accounts_where_files_must_be_copied = accounts_where_files_must_be_copied

    def get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_sync(result)
        return self._get_df_set_analysis_must_file_exist(result)

    def _get_df_set_analysis_sync(self, df: Df) -> Df:
        result = df
        for aws_account_target in self._accounts_where_files_must_be_copied:
            result = _AccountSyncDfAnalysis(self._aws_account_origin, aws_account_target, result).get_df_set_analysis()
        return result

    def _get_df_set_analysis_must_file_exist(self, df: Df) -> Df:
        return _TargetAccountWithoutMoreFilesDfAnalysis(
            self._aws_account_origin, self._aws_account_that_must_not_have_more_files, df
        ).get_df_set_analysis()


class _AccountSyncDfAnalysis:
    def __init__(
        self,
        aws_account_origin: str,
        aws_account_target: str,
        df: Df,
    ):
        self._df = df
        self._column_name_result = f"is_sync_ok_in_{aws_account_target}"
        self._condition = _AnalysisCondition(aws_account_origin, aws_account_target, df)

    def get_df_set_analysis(self) -> Df:
        result = self._df.copy()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        result[
            [
                ("analysis", self._column_name_result),
            ]
        ] = None
        for condition_result, condition in self._get_result_and_condition().items():
            result.loc[
                condition,
                [
                    ("analysis", self._column_name_result),
                ],
            ] = condition_result
        return result

    def _get_result_and_condition(self) -> dict:
        return {
            False: self._condition.condition_sync_is_wrong,
            True: self._condition.condition_sync_is_ok,
            "No file to sync": ~self._condition.condition_exists_file_to_sync,
        }


class _TargetAccountWithoutMoreFilesDfAnalysis:
    def __init__(
        self,
        aws_account_origin: str,
        aws_account_target: str,
        df: Df,
    ):
        self._column_name_result = f"must_exist_in_{aws_account_target}"
        self._df = df
        self._condition = _AnalysisCondition(aws_account_origin, aws_account_target, df)

    def get_df_set_analysis(self) -> Df:
        result = self._df.copy()
        result[
            [
                ("analysis", self._column_name_result),
            ]
        ] = None
        for condition_result, condition in self._get_result_and_condition().items():
            result.loc[
                condition,
                [
                    ("analysis", self._column_name_result),
                ],
            ] = condition_result
        return result

    def _get_result_and_condition(self) -> dict:
        return {False: self._condition.condition_must_not_exist}


class _AnalysisCondition:
    def __init__(
        self,
        aws_account_origin: str,
        aws_account_target: str,
        df: Df,
    ):
        self._aws_account_origin = aws_account_origin
        self._aws_account_target = aws_account_target
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
        return self._df.loc[:, (self._aws_account_origin, "size")].notnull()

    @property
    def _condition_file_is_sync(self) -> Series:
        return (
            self._df.loc[:, (self._aws_account_origin, "size")] == self._df.loc[:, (self._aws_account_target, "size")]
        )

    @property
    def _condition_exists_file_in_target_aws_account(self) -> Series:
        return self._df.loc[:, (self._aws_account_target, "size")].notnull()


def _show_summary(aws_account_with_data_to_sync: str, accounts_where_files_must_be_copied: list[str], df: Df):
    for aws_account_to_compare in accounts_where_files_must_be_copied:
        column_name_compare_result = f"is_sync_ok_in_{aws_account_to_compare}"
        condition = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
            df.loc[:, ("analysis", column_name_compare_result)].eq(False)
        )
        result = df[condition]
        print(f"Files not copied in {aws_account_to_compare} ({len(result)}):")
        print(result)


class _AnalysisDfToCsv:
    def export(self, df: Df, file_path_name: str):
        csv_df = self._get_df_to_export(df)
        csv_df.to_csv(file_path_name)

    def _get_df_to_export(self, df: Df) -> Df:
        result = df.copy()
        csv_column_names = ["_".join(values) for values in result.columns]
        csv_column_names = [
            self._get_csv_column_name_drop_undesired_text(column_name) for column_name in csv_column_names
        ]
        result.columns = csv_column_names
        result.index.names = ["bucket", "file_path_in_s3", "file_name"]
        return result

    def _get_csv_column_name_drop_undesired_text(self, column_name: str) -> str:
        if column_name.startswith("analysis_"):
            return column_name.replace("analysis_", "", 1)
        return column_name
