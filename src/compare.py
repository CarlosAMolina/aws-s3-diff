from pandas import DataFrame as Df

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
        aws_account_with_data_to_sync = _get_aws_account_with_data_to_sync()
        for aws_account_to_compare in _get_accounts_where_files_must_be_copied():
            condition_sync_wrong_in_account = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
                df.loc[:, (aws_account_with_data_to_sync, "size")] != df.loc[:, (aws_account_to_compare, "size")]
            )
            condition_sync_ok_in_account = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
                df.loc[:, (aws_account_with_data_to_sync, "size")] == df.loc[:, (aws_account_to_compare, "size")]
            )
            condition_sync_not_required = df.loc[:, (aws_account_with_data_to_sync, "size")].isnull()
            # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
            column_name_result = f"is_sync_ok_in_{aws_account_to_compare}"
            df[
                [
                    ("analysis", column_name_result),
                ]
            ] = None
            for condition_and_result in (
                (condition_sync_wrong_in_account, False),
                (condition_sync_ok_in_account, True),
                (condition_sync_not_required, "No file to sync"),
            ):
                condition, result = condition_and_result
                df.loc[
                    condition,
                    [
                        ("analysis", column_name_result),
                    ],
                ] = result
        return df

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


def _get_aws_account_with_data_to_sync() -> str:
    return S3UrisFileReader().get_aws_accounts()[0]


def _get_aws_account_that_must_not_have_more_files() -> str:
    for aws_account in LocalResults()._get_aws_accounts_analyzed():
        if aws_account.startswith("aws_account_2"):
            return aws_account
    raise ValueError("No aws account that must not have more files")


def _get_accounts_where_files_must_be_copied() -> list[str]:
    result = LocalResults()._get_aws_accounts_analyzed()
    aws_account_with_data_to_sync = _get_aws_account_with_data_to_sync()
    result.remove(aws_account_with_data_to_sync)
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
