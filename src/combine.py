from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df
from pandas import MultiIndex

from local_results import LocalResults
from s3_uris_to_analyze import S3UrisFileReader
from types_custom import AllAccoutsS3DataDf


def get_df_s3_data_all_accounts_from_accounts_individual_results() -> AllAccoutsS3DataDf:
    result = _get_df_combine_aws_accounts_results()
    return _get_df_drop_incorrect_empty_rows(result)


def _get_df_combine_aws_accounts_results() -> Df:
    aws_accounts = S3UrisFileReader().get_aws_accounts()
    result = _get_df_for_aws_account(aws_accounts[0])
    for aws_account in aws_accounts[1:]:
        account_df = _get_df_for_aws_account(aws_account)
        account_df = _S3UriDfModifier(aws_accounts[0], aws_account, account_df).get_df_set_s3_uris_in_origin_account()
        result = result.join(account_df, how="outer")
    return result


def _get_df_for_aws_account(aws_account: str) -> Df:
    local_file_path_name = LocalResults().get_file_path_aws_account_results(aws_account)
    result = _get_df_aws_account_from_file(local_file_path_name)
    result.columns = MultiIndex.from_tuples(_get_column_names_mult_index(aws_account, list(result.columns)))
    return result


class _S3UriDfModifier:
    def __init__(self, *args):
        self._aws_account_origin, self._aws_account_target, self._df = args
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self) -> Df:
        s3_uris_map_df = self._get_df_s3_uris_map_between_accounts()
        return self._get_df_modify_buckets_and_paths(s3_uris_map_df)

    def _get_df_s3_uris_map_between_accounts(self) -> Df:
        return self._s3_uris_file_reader.get_df_file_what_to_analyze()[
            [self._aws_account_origin, self._aws_account_target]
        ]

    def _get_df_modify_buckets_and_paths(self, s3_uris_map_df: Df) -> Df:
        result = self._df.copy()
        new_multi_index_as_tuples = self._get_new_multi_index_as_tuples(result.index.tolist(), s3_uris_map_df)
        result.index = MultiIndex.from_tuples(new_multi_index_as_tuples, names=result.index.names)
        return result

    def _get_new_multi_index_as_tuples(self, old_multi_index_as_tuples: list[tuple], s3_uris_map_df: Df) -> list[tuple]:
        # TODO use pandas join instead of foor loop
        return [
            self._get_new_multi_index_as_tuple(old_multi_index_as_tuple, s3_uris_map_df)
            for old_multi_index_as_tuple in old_multi_index_as_tuples
        ]

    def _get_new_multi_index_as_tuple(self, old_multi_index_as_tuple: tuple, s3_uris_map_df: Df) -> tuple:
        old_bucket, old_prefix, old_file_name = old_multi_index_as_tuple
        # TODO add test for url ending with and without `/`.
        # TODO required `r` string?
        s3_uris_map_for_current_index_df: Df = s3_uris_map_df[
            s3_uris_map_df[self._aws_account_target].str.contains(rf"s3://{old_bucket}/{old_prefix}/?")
        ]
        if s3_uris_map_for_current_index_df.empty:
            raise ValueError("Unmatched value")
        s3_uri_to_use = s3_uris_map_for_current_index_df[self._aws_account_origin].values[0]
        query_to_use = self._s3_uris_file_reader.get_s3_query_from_s3_uri(s3_uri_to_use)
        return (query_to_use.bucket, query_to_use.prefix, old_file_name)


def _get_column_names_mult_index(aws_account: str, column_names: list[str]) -> list[tuple[str, str]]:
    return [(aws_account, column_name) for column_name in column_names]


def _get_df_aws_account_from_file(file_path_name: Path) -> Df:
    return pd.read_csv(
        file_path_name,
        index_col=["bucket", "prefix", "name"],
        parse_dates=["date"],
    ).astype({"size": "Int64"})


def _get_df_drop_incorrect_empty_rows(df: Df) -> Df:
    """
    Drop null rows caused when merging query results without files in some accounts.
    Avoid drop queries without results in any aws account.
    """
    result = df
    count_files_per_bucket_and_path_df = (
        Df(result.index.to_list(), columns=result.index.names).groupby(["bucket", "prefix"]).count()
    )
    count_files_per_bucket_and_path_df.columns = MultiIndex.from_tuples(
        [
            ("count", "files_in_bucket_prefix"),
        ]
    )
    result = result.join(count_files_per_bucket_and_path_df)
    result = result.reset_index()
    result = result.loc[(~result["name"].isna()) | (result[("count", "files_in_bucket_prefix")] == 0)]
    result = result.set_index(["bucket", "prefix", "name"])
    return result.drop(columns=(("count", "files_in_bucket_prefix")))
