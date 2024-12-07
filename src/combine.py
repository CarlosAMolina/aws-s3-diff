from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df

from local_results import LocalResults
from s3_uris_to_analyze import S3UrisFileReader
from types_custom import S3Query


def get_df_combine_files() -> Df:
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


# TODO when reading the uris to check, assert all accounts have defined the same number of paths to analyze.


def _get_df_for_aws_account(aws_account: str) -> Df:
    local_file_path_name = LocalResults().get_file_path_aws_account_results(aws_account)
    result = _get_df_from_file(local_file_path_name)
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(aws_account, list(result.columns)))
    return result


class _S3UriDfModifier:
    def __init__(self, *args):
        self._aws_account_origin, self._aws_account_target, self._df = args
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_df_set_s3_uris_in_origin_account(self) -> Df:
        # TODO rm
        if self._aws_account_target != "aws_account_3_dev":
            return self._df
        s3_uris_df = self._get_df_s3_uris_map_between_accounts()
        result = self._df.copy()
        for s3_uri_accounts_map in s3_uris_df.itertuples():
            query_to_use = self._get_s3_query_from_s3_uri_accounts_map(self._aws_account_origin, s3_uri_accounts_map)
            query_to_replace = self._get_s3_query_from_s3_uri_accounts_map(
                self._aws_account_target, s3_uri_accounts_map
            )
            if query_to_use == query_to_replace:
                continue
            print(query_to_use)  # TODO
            print(query_to_replace)  # TODO
            print(result)  # TODO
            # TODO replace index
            breakpoint()
        return result

    def _get_df_s3_uris_map_between_accounts(self) -> Df:
        return self._s3_uris_file_reader.get_df_file_what_to_analyze()[
            [self._aws_account_origin, self._aws_account_target]
        ]

    def _get_s3_query_from_s3_uri_accounts_map(self, aws_account: str, s3_uri_accounts_map: tuple) -> S3Query:
        s3_uri = getattr(s3_uri_accounts_map, aws_account)
        return self._s3_uris_file_reader.get_s3_query_from_s3_uri(s3_uri)


def _get_column_names_mult_index(aws_account: str, column_names: list[str]) -> list[tuple[str, str]]:
    return [(aws_account, column_name) for column_name in column_names]


# TODO rename specify _aws_account_results_file
def _get_df_from_file(file_path_name: Path) -> Df:
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
    count_files_per_bucket_and_path_df.columns = pd.MultiIndex.from_tuples(
        [
            ("count", "files_in_bucket_prefix"),
        ]
    )
    result = result.join(count_files_per_bucket_and_path_df)
    result = result.reset_index()
    result = result.loc[(~result["name"].isna()) | (result[("count", "files_in_bucket_prefix")] == 0)]
    result = result.set_index(["bucket", "prefix", "name"])
    return result.drop(columns=(("count", "files_in_bucket_prefix")))
