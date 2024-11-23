import sys

from config import get_s3_uris_file_reader
from local_results import LocalResults


def run():
    _IteractiveMenu().run()


class _IteractiveMenu:
    def __init__(self):
        self._s3_uris_file_reader = get_s3_uris_file_reader()
        self._local_results = LocalResults()

    def run(self):
        print("Welcome to the AWS S3 Diff tool!")
        self._show_aws_accounts_to_analyze()
        if self._local_results.get_aws_account_index_to_analyze() > 2:
            raise ValueError("All AWS accounts have been analyzed")
        if self._local_results.get_aws_account_index_to_analyze() == 0:
            self._local_results.create_analysis_results_folder_if_required(
                self._s3_uris_file_reader.get_number_of_aws_accounts()
            )
        aws_account = self._get_aws_account_to_analyze()
        print(f"The following AWS account will be analyzed: {aws_account}")
        self._exit_program_if_no_aws_credentials_in_terminal()
        _AccountAnalyzer(aws_account).run()

    def _show_aws_accounts_to_analyze(self):
        print("AWS accounts configured to be analyzed:")
        aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        aws_accounts_list = [f"- {aws_account}" for aws_account in aws_accounts]
        print("\n".join(aws_accounts_list))

    def _get_aws_account_to_analyze(self) -> str:
        aws_account_index_to_analyze = self._local_results.get_aws_account_index_to_analyze()
        aws_accounts_to_analyze = self._s3_uris_file_reader.get_aws_accounts()
        return aws_accounts_to_analyze[aws_account_index_to_analyze]

    def _exit_program_if_no_aws_credentials_in_terminal(self):
        print("Have you generated in you terminal the AWS credentials to connect with that AWS account?")
        while True:
            user_input = input("Y/n: ").lower()
            if user_input == "n":
                print("Generate the credentials to work with that AWS account and run the program again")
                sys.exit()
            if user_input == "y" or len(user_input) == 0:
                return


class _AccountAnalyzer:
    def __init__(self, aws_account: str):
        self._aws_account = aws_account
        self._local_results = LocalResults()

    def run(self):
        print(f"Analyzing the account: {self._aws_account}")
        # TODO create the folder after retrieve aws results to avoid not use the folder if any aws error
        self._local_results.create_aws_account_results_folder(self._aws_account)
        # TODO continue adding code


if __name__ == "__main__":
    run()
