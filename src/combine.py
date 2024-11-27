from pathlib import Path

import pandas as pd
from pandas import DataFrame as Df

from local_results import LocalResults


def get_df_combine_files() -> Df:
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
