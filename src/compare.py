import os
from pathlib import PurePath

import pandas as pd
from pandas import DataFrame as Df

from config import AWS_ACCOUNT_WITH_DATA_TO_SYNC
from constants import MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS
from utils import get_aws_accounts

FilePathNamesToCompare = tuple[str, str, str]


def run():
    _run_file_name()


def _run_file_name():
    s3_data_df = _get_df_combine_files()
    s3_analyzed_df = _get_df_analyze_s3_data(s3_data_df)
    _show_summary(s3_analyzed_df)
    _export_to_csv(s3_analyzed_df)


def _get_df_combine_files() -> Df:
    result = Df()
    buckets_and_files: dict = _get_buckets_and_exported_files()
    for aws_account in get_aws_accounts():
        account_df = _get_df_combine_files_for_aws_account(aws_account, buckets_and_files)
        result = result.join(account_df, how="outer")
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(result.columns))
    result.index = pd.MultiIndex.from_tuples(_get_index_multi_index(result.index))
    return result


def _get_df_combine_files_for_aws_account(aws_account: str, buckets_and_files: dict) -> Df:
    result = Df()
    for bucket_name, file_names in buckets_and_files.items():
        for file_name in file_names:
            file_path_name = PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, aws_account, bucket_name, file_name)
            file_df = _get_df_from_file(file_path_name)
            file_df = file_df.add_prefix(f"{aws_account}_value_")
            file_df = file_df.set_index(f"{bucket_name}_path_{file_name}_file_" + file_df.index.astype(str))
            result = pd.concat([result, file_df])
    return result


def _get_buckets_and_exported_files() -> dict[str, list[str]]:
    bucket_names = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, AWS_ACCOUNT_WITH_DATA_TO_SYNC))
    bucket_names.sort()
    accounts = get_aws_accounts()
    accounts.remove(AWS_ACCOUNT_WITH_DATA_TO_SYNC)
    accounts.sort()
    for account in accounts:
        buckets_in_account = os.listdir(PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, account))
        buckets_in_account.sort()
        if bucket_names != buckets_in_account:
            raise ValueError(
                f"The S3 data has not been exported correctly. Error comparing buckets in account '{account}'"
            )
    result = {}
    for bucket in bucket_names:
        file_names = os.listdir(
            PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, AWS_ACCOUNT_WITH_DATA_TO_SYNC, bucket)
        )
        file_names.sort()
        for account in accounts:
            files_for_bucket_in_account = os.listdir(
                PurePath(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS, account, bucket)
            )
            files_for_bucket_in_account.sort()
            if file_names != files_for_bucket_in_account:
                raise ValueError(
                    f"The S3 data has not been exported correctly. Error comparing files in account '{account}' and bucket '{bucket}'"
                )
        result[bucket] = file_names
    return result


def _get_df_from_file(file_path_name: PurePath) -> Df:
    result = pd.read_csv(
        file_path_name,
        index_col="name",
        parse_dates=["date"],
    )
    return result


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


def _get_df_analyze_s3_data(df: Df) -> Df:
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied():
        condition_copied_wrong_in_account = (df.loc[:, (AWS_ACCOUNT_WITH_DATA_TO_SYNC, "size")].notnull()) & (
            df.loc[:, (AWS_ACCOUNT_WITH_DATA_TO_SYNC, "size")] != df.loc[:, (aws_account_to_compare, "size")]
        )
        # https://stackoverflow.com/questions/18470323/selecting-columns-from-pandas-multiindex
        column_name_compare_result = f"is_{AWS_ACCOUNT_WITH_DATA_TO_SYNC}_copied_ok_in_{aws_account_to_compare}"
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


def _get_accounts_where_files_must_be_copied() -> list[str]:
    result = get_aws_accounts()
    result.remove(AWS_ACCOUNT_WITH_DATA_TO_SYNC)
    return result


def _show_summary(df: Df):
    for aws_account_to_compare in _get_accounts_where_files_must_be_copied():
        column_name_compare_result = f"is_{AWS_ACCOUNT_WITH_DATA_TO_SYNC}_copied_ok_in_{aws_account_to_compare}"
        condition = (df.loc[:, (AWS_ACCOUNT_WITH_DATA_TO_SYNC, "size")].notnull()) & (
            df.loc[:, ("analysis", column_name_compare_result)].eq(False)
        )
        result = df[condition]
        print(f"Files not copied in {aws_account_to_compare} ({len(result)}):")
        print(result)


def _export_to_csv(df: Df):
    to_csv_df = df.copy()
    to_csv_df.columns = ["_".join(values) for values in to_csv_df.columns]
    to_csv_df.to_csv("/tmp/analysis.csv")


if __name__ == "__main__":
    run()
