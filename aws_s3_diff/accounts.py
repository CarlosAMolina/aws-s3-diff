from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.local_results import get_account_file_name
from aws_s3_diff.local_results import LocalResults


class AnalyzedAccounts:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    # TODO replace in all files the account variable with a call to this method
    def get_account_to_analyze(self) -> str:
        result_file_names = self._local_results.get_file_names_results()
        for account_to_analyze in self._s3_uris_file_reader.get_accounts():
            if get_account_file_name(account_to_analyze) not in result_file_names:
                return account_to_analyze
        # Unexpected situation. This method cannot be called if all accounts have been analyzed.
        raise RuntimeError("All AWS accounts have been analyzed")

    def have_all_accounts_been_analyzed(self) -> bool:
        result_file_names = self._local_results.get_file_names_results()
        for account_to_analyze in self._s3_uris_file_reader.get_accounts():
            if get_account_file_name(account_to_analyze) not in result_file_names:
                return False
        return True
