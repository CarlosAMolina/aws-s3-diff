from aws_s3_diff.config_file import S3UrisFileReader
from aws_s3_diff.local_result import get_account_file_name
from aws_s3_diff.local_result import LocalResults


def get_account_to_analyze() -> str:
    result_file_names = LocalResults().get_file_names_results()
    for account_to_analyze in S3UrisFileReader().get_accounts():
        if get_account_file_name(account_to_analyze) not in result_file_names:
            return account_to_analyze
    # Unexpected situation. This method cannot be called if all accounts have been analyzed.
    raise RuntimeError("All AWS accounts have been analyzed")


def have_all_accounts_been_analyzed() -> bool:
    result_file_names = LocalResults().get_file_names_results()
    for account_to_analyze in S3UrisFileReader().get_accounts():
        if get_account_file_name(account_to_analyze) not in result_file_names:
            return False
    return True
