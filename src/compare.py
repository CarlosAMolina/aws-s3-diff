import os
from pathlib import Path
from pathlib import PurePath

import pandas as pd
from pandas import DataFrame as Df

from config import Config

FilePathNamesToCompare = tuple[str, str, str]


def run():
    path_config_files = Path(__file__).parent.absolute()
    path_with_folder_exported_s3_data = Path(__file__).parent.absolute()
    config = Config(path_config_files, path_with_folder_exported_s3_data)
    _run_file_name(config)


def _run_file_name(config: Config):
    s3_data_df = _get_df_combine_files(config)
    s3_analyzed_df = _get_df_analyze_s3_data(config, s3_data_df)
    _show_summary(config, s3_analyzed_df)
    _CsvExporter().export(s3_analyzed_df, "/tmp/analysis.csv")


def _get_df_combine_files(config: Config) -> Df:
    result = Df()
    buckets_and_files: dict = _get_buckets_and_exported_files(config)
    for aws_account in config.get_aws_accounts():
        account_df = _get_df_combine_files_for_aws_account(aws_account, buckets_and_files, config)
        result = result.join(account_df, how="outer")
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(result.columns))
    result.index = pd.MultiIndex.from_tuples(_get_index_multi_index(result.index))
    return result


def _get_df_combine_files_for_aws_account(aws_account: str, buckets_and_files: dict, config: Config) -> Df:
    result = Df()
    for bucket_name, file_names in buckets_and_files.items():
        for file_name in file_names:
            file_path_name = config.get_path_exported_s3_data().joinpath(aws_account, bucket_name, file_name)
            file_df = _get_df_from_file(file_path_name)
            file_df = file_df.add_prefix(f"{aws_account}_value_")
            file_df = file_df.set_index(f"{bucket_name}_path_{file_name}_file_" + file_df.index.astype(str))
            result = pd.concat([result, file_df])
    return result


def _get_buckets_and_exported_files(config: Config) -> dict[str, list[str]]:
    aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
    bucket_names = os.listdir(config.get_path_exported_s3_data().joinpath(aws_account_with_data_to_sync))
    bucket_names.sort()
    accounts = config.get_aws_accounts()
    accounts.remove(aws_account_with_data_to_sync)
    accounts.sort()
    for account in accounts:
        buckets_in_account = os.listdir(config.get_path_exported_s3_data().joinpath(account))
        buckets_in_account.sort()
        if bucket_names != buckets_in_account:
            raise ValueError(
                f"The S3 data has not been exported correctly. Error comparing buckets in account '{account}'"
            )
    result = {}
    for bucket in bucket_names:
        file_names = os.listdir(config.get_path_exported_s3_data().joinpath(aws_account_with_data_to_sync, bucket))
        file_names.sort()
        for account in accounts:
            files_for_bucket_in_account = os.listdir(config.get_path_exported_s3_data().joinpath(account, bucket))
            files_for_bucket_in_account.sort()
            if file_names != files_for_bucket_in_account:
                raise ValueError(
                    "The S3 data has not been exported correctly"
                    f". Error comparing files in account '{account}' and bucket '{bucket}'"
                )
        result[bucket] = file_names
    return result


def _get_df_from_file(file_path_name: PurePath) -> Df:
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
    path_name = path_name.replace(".csv", "")
    return bucket_name, path_name, file_name


def _get_df_analyze_s3_data(config: Config, df: Df) -> Df:
    aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied(config):
        condition_copied_wrong_in_account = (df.loc[:, (aws_account_with_data_to_sync, "size")].notnull()) & (
            df.loc[:, (aws_account_with_data_to_sync, "size")] != df.loc[:, (aws_account_to_compare, "size")]
        )
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        column_name_compare_result = f"is_{aws_account_with_data_to_sync}_copied_ok_in_{aws_account_to_compare}"
        df[
            [
                ("analysis", column_name_compare_result),
            ]
        ] = None
        df.loc[
            condition_copied_wrong_in_account,
            [
                ("analysis", column_name_compare_result),
            ],
        ] = False
    return df


def _get_accounts_where_files_must_be_copied(config: Config) -> list[str]:
    result = config.get_aws_accounts()
    aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
    result.remove(aws_account_with_data_to_sync)
    return result


def _show_summary(config: Config, df: Df):
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied(config):
        aws_account_with_data_to_sync = config.get_aws_account_with_data_to_sync()
        column_name_compare_result = f"is_{aws_account_with_data_to_sync}_copied_ok_in_{aws_account_to_compare}"
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
        result.columns = ["_".join(values) for values in result.columns]
        return result


if __name__ == "__main__":
    run()
