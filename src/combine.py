from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df

from local_results import LocalResults


def get_df_combine_files() -> Df:
    result = _get_df_combine_aws_accounts_results()
    return _get_df_drop_incorrect_empty_rows(result)


def _get_df_combine_aws_accounts_results() -> Df:
    aws_accounts = LocalResults()._get_aws_accounts_analyzed()
    result = _get_df_for_aws_account(aws_accounts[0])
    for aws_account in aws_accounts[1:]:
        account_df = _get_df_for_aws_account(aws_account)
        result = result.join(account_df, how="outer")
    return result


# TODO when reading the uris to check, assert all accounts all paths to analyze.


def _get_df_for_aws_account(aws_account: str) -> Df:
    local_file_path_name = LocalResults().get_file_path_aws_account_results(aws_account)
    result = _get_df_from_file(local_file_path_name)
    result.columns = pd.MultiIndex.from_tuples(_get_column_names_mult_index(aws_account, list(result.columns)))
    return result


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
    Drop null rows provoced by queries without results in some accounts.
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
