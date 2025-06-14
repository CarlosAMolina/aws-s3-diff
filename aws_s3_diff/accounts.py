from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.local_results import LocalResults


class AnalyzedAccounts:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    # TODO replace in all files the account variable with a call to this method
    def get_account_to_analyze(self) -> str:
        for account_to_analyze in self._s3_uris_file_reader.get_accounts():
            if not self._local_results.has_this_account_been_analyzed(account_to_analyze):
                return account_to_analyze
        # Unexpected situation. This method cannot be called if all accounts have been analyzed.
        raise RuntimeError("All AWS accounts have been analyzed")

    def have_all_accounts_been_analyzed(self) -> bool:
        for account_to_analyze in self._s3_uris_file_reader.get_accounts():
            if not self._local_results.has_this_account_been_analyzed(account_to_analyze):
                return False
        return True
