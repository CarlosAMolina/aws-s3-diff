from local_results import LocalResults

_AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX = "aws_account_1"
_AWS_ACCOUNT_WITHOUT_MORE_FILES_PREFIX = "aws_account_2"


def get_aws_account_with_data_to_sync() -> str:
    for aws_account in LocalResults()._get_aws_accounts_analyzed():
        if aws_account.startswith(_AWS_ACCOUNT_WITH_DATA_TO_SYNC_PREFIX):
            return aws_account
    raise ValueError("No aws account to sync")


def get_aws_account_that_must_not_have_more_files() -> str:
    for aws_account in LocalResults()._get_aws_accounts_analyzed():
        if aws_account.startswith(_AWS_ACCOUNT_WITHOUT_MORE_FILES_PREFIX):
            return aws_account
    raise ValueError("No aws account that must not have more files")
