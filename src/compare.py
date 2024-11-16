import os
from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df

from config import Config

FilePathNamesToCompare = tuple[str, str, str]


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
    result = Df()
    buckets_and_files: dict = _get_buckets_and_exported_files(config)
    for aws_account in config.get_aws_accounts():
        account_df = _get_df_combine_files_for_aws_account(aws_account, buckets_and_files, config)
        result = result.join(account_df, how="outer")
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/advanced.html#creating-a-multiindex-hierarchical-index-object
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(result.columns))
    result.index = pd.MultiIndex.from_tuples(
        _get_index_multi_index(result.index), names=["bucket", "file_path_in_s3", "file_name"]
    )
    return result


def _get_buckets_and_exported_files(config: Config) -> dict[str, list[str]]:
    aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
    bucket_names = os.listdir(
        config.get_local_path_directory_results_to_compare().joinpath(aws_account_with_data_to_sync)
    )
    bucket_names.sort()
    accounts = config.get_aws_accounts()
    accounts.remove(aws_account_with_data_to_sync)
    accounts.sort()
    for account in accounts:
        buckets_in_account = os.listdir(config.get_local_path_directory_results_to_compare().joinpath(account))
        buckets_in_account.sort()
        if bucket_names != buckets_in_account:
            raise ValueError(
                f"The S3 data has not been exported correctly. Error comparing buckets in account '{account}'"
            )
    result = {}
    for bucket in bucket_names:
        file_names = os.listdir(
            config.get_local_path_directory_results_to_compare().joinpath(aws_account_with_data_to_sync, bucket)
        )
        file_names.sort()
        for account in accounts:
            files_for_bucket_in_account = os.listdir(
                config.get_local_path_directory_results_to_compare().joinpath(account, bucket)
            )
            files_for_bucket_in_account.sort()
            if file_names != files_for_bucket_in_account:
                raise ValueError(
                    "The S3 data has not been exported correctly"
                    f". Error comparing files in account '{account}' and bucket '{bucket}'"
                )
        result[bucket] = file_names
    return result


def _get_df_combine_files_for_aws_account(aws_account: str, buckets_and_local_files: dict, config: Config) -> Df:
    result = Df()
    for bucket_name, local_file_names in buckets_and_local_files.items():
        for local_file_name in local_file_names:
            local_file_path_name = config.get_local_path_directory_results_to_compare().joinpath(
                aws_account, bucket_name, local_file_name
            )
            file_df = _get_df_from_file(local_file_path_name)
            # This `if` avoids Pandas's future warning message: https://github.com/pandas-dev/pandas/issues/55928
            if file_df.empty:
                print(
                    f"Account {aws_account} and bucket {bucket_name} without files for"
                    f" {config.get_s3_key_from_results_local_file(local_file_name)}. Omitting"
                )
            else:
                file_df = file_df.add_prefix(f"{aws_account}_value_")
                file_df = _get_file_df_update_index(bucket_name, config, file_df, local_file_name)
                result = pd.concat([result, file_df])
    return result


def _get_file_df_update_index(bucket_name: str, config: Config, df: Df, local_file_name: str) -> Df:
    s3_key = config.get_s3_key_from_results_local_file(local_file_name)
    index_prefix = f"{bucket_name}_path_{s3_key}_file_"
    result = df.copy()
    # TODO Ensure s3 path appears in the result despite it doesn't have files in any aws account.
    # TODO instead to add paths without files here, do it when all the aws accounts have been
    # TODO analized and only if the apth doesn't have file for all accounts, in other case it will
    # TODO add a wrong empty line to the final result
    # TODO in the e2e tests check a s3 path without file in any aws account to assert it
    # TODO appears in the final result. This can be added like this:
    # TODO ```python
    # TODO if result.empty:
    # TODO     data ={column_name: [None] for column_name in result.columns}
    # TODO     return pd.DataFrame(data=data, index=pd.Index([index_prefix]))
    # TODO ```
    return result.set_index(index_prefix + result.index.astype(str))


def _get_df_from_file(file_path_name: Path) -> Df:
    return pd.read_csv(
        file_path_name,
        index_col="name",
        parse_dates=["date"],
    ).astype({"size": "Int64"})


def _get_column_names_mult_index(column_names: list[str]) -> list[tuple[str, str]]:
    return [_get_tuple_column_names_multi_index(column_name) for column_name in column_names]


def _get_tuple_column_names_multi_index(column_name: str) -> tuple[str, str]:
    aws_account, file_value = column_name.split("_value_")
    return aws_account, file_value


def _get_index_multi_index(indexes: list[str]) -> list[tuple[str, str, str]]:
    return [_get_tuple_index_multi_index(index) for index in indexes]


def _get_tuple_index_multi_index(index: str) -> tuple[str, str, str]:
    bucket_name, path_and_file_name = index.split("_path_")
    path_name, file_name = path_and_file_name.split("_file_")
    return bucket_name, path_name, file_name


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
    result = config.get_aws_accounts()
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
        return result

    def _get_csv_column_name_drop_undesired_text(self, column_name: str) -> str:
        if column_name.startswith("analysis_"):
            return column_name.replace("analysis_", "", 1)
        return column_name
