from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df

from config import Config
from local_results import LocalResults


class S3DataComparator:
    def run(self, config: Config):
        s3_analyzed_df = self._get_df_s3_data_analyzed(config)
        _show_summary(config, s3_analyzed_df)
        # TODO save in this projects instead of in /tmp
        _CsvExporter().export(s3_analyzed_df, "/tmp/analysis.csv")

    def _get_df_s3_data_analyzed(self, config: Config) -> Df:
        s3_data_df = _get_df_combine_files(config)
        return _S3DataAnalyzer(config).get_df_set_analysis_columns(s3_data_df)


def _get_df_combine_files(config: Config) -> Df:
    aws_accounts = LocalResults()._get_aws_accounts_analyzed()
    result = _get_df_for_aws_account(aws_accounts[0])
    for aws_account in aws_accounts[1:]:
        account_df = _get_df_for_aws_account(aws_account)
        result = result.join(account_df, how="outer")
    # TODO not drop only if its the only bucket and prefix, in
    # TODO order to maintain empty query results. And add this situtation to the tests
    return result.dropna(axis="index", how="all")


# TODO when reading the uris to check, assert all accounts all paths to analyze.


def _get_df_for_aws_account(aws_account: str) -> Df:
    local_file_path_name = LocalResults().get_file_path_aws_account_results(aws_account)
    result = _get_df_from_file(local_file_path_name)
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(aws_account, list(result.columns)))
    return result


def _get_column_names_mult_index(aws_account: str, column_names: list[str]) -> list[tuple[str, str]]:
    return [(aws_account, column_name) for column_name in column_names]


# TODO Ensure s3 path appears in the result despite it doesn't have files in any aws account.
# TODO Maybe, instead to add paths without files here, do it when all the aws accounts have been
# TODO analized and only if the path doesn't have file for all accounts, in other case it will
# TODO add a wrong empty line to the final result
# TODO in the e2e tests check a s3 path without file in any aws account to assert it
# TODO appears in the final result. This can be added like this:
# TODO ```python
# TODO if result.empty:
# TODO     data ={column_name: [None] for column_name in result.columns}
# TODO     return pd.DataFrame(data=data, index=pd.Index([index_prefix]))
# TODO ```


def _get_df_from_file(file_path_name: Path) -> Df:
    return pd.read_csv(
        file_path_name,
        index_col=["bucket", "prefix", "name"],
        parse_dates=["date"],
    ).astype({"size": "Int64"})


class _S3DataAnalyzer:
    def __init__(self, config: Config):
        self._config = config

    def get_df_set_analysis_columns(self, df: Df) -> Df:
        result = df.copy()
        result = self._get_df_set_analysis_sync(result)
        return self._get_df_set_analysis_must_file_exist(result)

    def _get_df_set_analysis_sync(self, df: Df) -> Df:
        aws_account_with_data_to_sync = self._config.get_aws_account_with_data_to_sync()
        for aws_account_to_compare in _get_accounts_where_files_must_be_copied(self._config):
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
        aws_account_with_data_to_sync = self._config.get_aws_account_with_data_to_sync()
        aws_account_without_more_files = self._config.get_aws_account_that_must_not_have_more_files()
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


def _get_accounts_where_files_must_be_copied(config: Config) -> list[str]:
    result = LocalResults()._get_aws_accounts_analyzed()
    aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
    result.remove(aws_account_with_data_to_sync)
    return result


def _show_summary(config: Config, df: Df):
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied(config):
        aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
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
