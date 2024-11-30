from pandas import DataFrame as Df
from pandas import Series

from analysis import AnalysisDfToCsv
from combine import get_df_combine_files
from local_results import LocalResults
from s3_uris_to_analyze import S3UrisFileReader


class S3DataComparator:
    def run(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        _show_summary(s3_analyzed_df)
        # TODO save in this projects instead of in /tmp
        AnalysisDfToCsv().export(s3_analyzed_df, "/tmp/analysis.csv")

    def _get_df_s3_data_analyzed(self) -> Df:
        s3_data_df = get_df_combine_files()
        return _S3DataAnalyzer().get_df_set_analysis_columns(s3_data_df)


class _S3DataAnalyzer:
    def get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_sync(result)
        return self._get_df_set_analysis_must_file_exist(result)

    def _get_df_set_analysis_sync(self, df: Df) -> Df:
        result = df
        aws_account_origin = _get_aws_account_with_data_to_sync()
        for aws_account_target in _get_accounts_where_files_must_be_copied():
            result = self._get_df_set_analysis_sync_from_account_to_account(
                result, aws_account_origin, aws_account_target
            )
        return result

    def _get_df_set_analysis_sync_from_account_to_account(
        self, df: Df, aws_account_origin: str, aws_account_target: str
    ) -> Df:
        return _AccountSyncAnalysis(aws_account_origin, aws_account_target, df).get_df_set_analysis(df)

    def _get_df_set_analysis_must_file_exist(self, df: Df) -> Df:
        aws_account_with_data_to_sync = _get_aws_account_with_data_to_sync()
        aws_account_without_more_files = _get_aws_account_that_must_not_have_more_files()
        column_name_result = f"must_exist_in_{aws_account_without_more_files}"
        df[
            [
                ("analysis", column_name_result),
            ]
        ] = None
        condition_must_not_exist = (df.loc[:, (aws_account_with_data_to_sync, "size")].isnull()) & (
            df.loc[:, (aws_account_without_more_files, "size")].notnull()
        )
        for condition_and_result in ((condition_must_not_exist, False),):
            condition, result = condition_and_result
            df.loc[
                condition,
                [
                    ("analysis", column_name_result),
                ],
            ] = result
        return df


class _AccountSyncAnalysis:
    def __init__(
        self,
        aws_account_origin: str,
        aws_account_target: str,
        df: Df,
    ):
        self._aws_account_origin = aws_account_origin
        self._aws_account_target = aws_account_target
        self._df = df

    def get_df_set_analysis(
        self,
        df: Df,
    ) -> Df:
        condition_sync_is_ok = (df.loc[:, (self._aws_account_origin, "size")].notnull()) & (
            df.loc[:, (self._aws_account_origin, "size")] == df.loc[:, (self._aws_account_target, "size")]
        )
        condition_sync_not_required = df.loc[:, (self._aws_account_origin, "size")].isnull()
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        df[
            [
                ("analysis", self._column_name_result),
            ]
        ] = None
        for result, condition in {
            False: self._condition_sync_is_wrong,
            True: condition_sync_is_ok,
            "No file to sync": condition_sync_not_required,
        }.items():
            df.loc[
                condition,
                [
                    ("analysis", self._column_name_result),
                ],
            ] = result
        return df

    @property
    def _condition_sync_is_wrong(self) -> Series:
        return (self._df.loc[:, (self._aws_account_origin, "size")].notnull()) & (
            self._df.loc[:, (self._aws_account_origin, "size")] != self._df.loc[:, (self._aws_account_target, "size")]
        )

    @property
    def _column_name_result(self) -> str:
        return f"is_sync_ok_in_{self._aws_account_target}"


def _get_aws_account_with_data_to_sync() -> str:
    return S3UrisFileReader().get_aws_accounts()[0]


def _get_aws_account_that_must_not_have_more_files() -> str:
    for aws_account in LocalResults()._get_aws_accounts_analyzed():
        if aws_account.startswith("aws_account_2"):
            return aws_account
    raise ValueError("No aws account that must not have more files")


def _get_accounts_where_files_must_be_copied() -> list[str]:
    result = S3UrisFileReader().get_aws_accounts()
    result.remove(_get_aws_account_with_data_to_sync())
    return result


def _show_summary(df: Df):
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied():
        aws_account_with_data_to_sync = _get_aws_account_with_data_to_sync()
        column_name_compare_result = f"is_sync_ok_in_{aws_account_to_compare}"
        condition = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
            df.loc[:, ("analysis", column_name_compare_result)].eq(False)
        )
        result = df[condition]
        print(f"Files not copied in {aws_account_to_compare} ({len(result)}):")
        print(result)
