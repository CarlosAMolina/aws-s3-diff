import sys

from analysis import S3DataAnalyzer
from local_results import LocalResults
from s3_extract import AwsAccountExtractor
from s3_uris_to_analyze import S3UrisFileChecker
from s3_uris_to_analyze import S3UrisFileReader


def run():
    _IteractiveMenu().run()


class _IteractiveMenu:
    def __init__(self):
        self._s3_uris_file_reader = S3UrisFileReader()
        self._local_results = LocalResults()

    def run(self):
        print("Welcome to the AWS S3 Diff tool!")
        print("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_aws_accounts_to_analyze()
        if self._have_all_aws_account_been_analyzed():
            print("All AWS accounts have been analyzed. Starting a new analysis")
            self._local_results.remove_file_with_analysis_date()
        aws_account = self._get_aws_account_to_analyze()
        print(f"The following AWS account will be analyzed: {aws_account}")
        self._exit_program_if_no_aws_credentials_in_terminal()
        if self._local_results.get_aws_account_index_to_analyze() == 0:
            self._local_results.create_analysis_results_folder()
        self._extract_aws_account_information(aws_account)
        if self._have_all_aws_account_been_analyzed():
            S3DataAnalyzer().run()
            LocalResults().remove_file_with_analysis_date()

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
        print("Have you generated in you terminal the AWS credentials to authenticate in that AWS account?")
        while True:
            user_input = input("Y/n: ").lower()
            if user_input == "n":
                print("Generate the credentials to work with that AWS account and run the program again")
                sys.exit()
            if user_input == "y" or len(user_input) == 0:
                return

    def _extract_aws_account_information(self, aws_account: str):
        AwsAccountExtractor(
            self._local_results.get_file_path_aws_account_results(aws_account),
            S3UrisFileReader().get_s3_queries_for_aws_account(aws_account),
        ).extract()

    def _have_all_aws_account_been_analyzed(self) -> bool:
        return (
            self._local_results.get_aws_account_index_to_analyze()
            == self._s3_uris_file_reader.get_number_of_aws_accounts()
        )


if __name__ == "__main__":
    run()
