from pandas import DataFrame as Df

from analysis import get_aws_account_that_must_not_have_more_files
from analysis import get_aws_account_with_data_to_sync
from combine import get_df_combine_files
from local_results import LocalResults


class S3DataComparator:
    def run(self):
        s3_analyzed_df = self._get_df_s3_data_analyzed()
        _show_summary(s3_analyzed_df)
        # TODO save in this projects instead of in /tmp
        _CsvExporter().export(s3_analyzed_df, "/tmp/analysis.csv")

    def _get_df_s3_data_analyzed(self) -> Df:
        s3_data_df = get_df_combine_files()
        return _S3DataAnalyzer().get_df_set_analysis_columns(s3_data_df)


class _S3DataAnalyzer:
    def get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_sync(result)
        return self._get_df_set_analysis_must_file_exist(result)

    def _get_df_set_analysis_sync(self, df: Df) -> Df:
        aws_account_with_data_to_sync = get_aws_account_with_data_to_sync()
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
        aws_account_with_data_to_sync = get_aws_account_with_data_to_sync()
        aws_account_without_more_files = get_aws_account_that_must_not_have_more_files()
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


def _get_accounts_where_files_must_be_copied() -> list[str]:
    result = LocalResults()._get_aws_accounts_analyzed()
    aws_account_with_data_to_sync = get_aws_account_with_data_to_sync()
    result.remove(aws_account_with_data_to_sync)
    return result


def _show_summary(df: Df):
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied():
        aws_account_with_data_to_sync = get_aws_account_with_data_to_sync()
        column_name_compare_result = f"is_sync_ok_in_{aws_account_to_compare}"
        condition = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
            df.loc[:, ("analysis", column_name_compare_result)].eq(False)
        )
        result = df[condition]
        print(f"Files not copied in {aws_account_to_compare} ({len(result)}):")
        print(result)


class _CsvExporter:
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
