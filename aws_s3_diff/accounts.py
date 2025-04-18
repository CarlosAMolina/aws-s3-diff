from config_files import S3UrisFileReader
from local_results import LocalResults


class AnalyzedAccounts:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    # TODO replace in all files the account variable with a call to this method
    def get_account_to_analyze(self) -> str:
        accounts_to_analyze = self._s3_uris_file_reader.get_accounts()
        last_account_analyzed = self._get_last_account_analyzed()
        if last_account_analyzed is None:
            return self._s3_uris_file_reader.get_first_account()
        if last_account_analyzed == self._s3_uris_file_reader.get_last_account():
            # Unexpected situation. This method cannot be called if all accounts have been analyzed.
            raise RuntimeError("All AWS accounts have been analyzed")
        return accounts_to_analyze[accounts_to_analyze.index(last_account_analyzed) + 1]

    def have_all_accounts_been_analyzed(self) -> bool:
        return self._get_last_account_analyzed() == self._s3_uris_file_reader.get_last_account()

    def _get_last_account_analyzed(self) -> str | None:
        result = None
        for account in self._s3_uris_file_reader.get_accounts():
            if not self._local_results.has_this_account_been_analyzed(account):
                return result
            result = account
        return result
