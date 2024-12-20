import sys
from abc import ABC
from abc import abstractmethod

from analysis import S3DataAnalyzer
from local_results import LocalResults
from s3_data import export_s3_data_all_accounts_to_one_file
from s3_data import export_s3_data_of_account
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
        _get_aws_account_process().run()
        if self._have_all_aws_account_been_analyzed():
            # This condition avoids generating the combination file if it exists.
            # For example: the user drops the analysis file in order to run the program and generate the analysis again.
            if not self._local_results.analysis_paths.file_s3_data_all_accounts.is_file():
                _get_aws_accounts_combination_process().run()
            _get_analysis_process().run()

    def _show_aws_accounts_to_analyze(self):
        print("AWS accounts configured to be analyzed:")
        aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        aws_accounts_list = [f"- {aws_account}" for aws_account in aws_accounts]
        print("\n".join(aws_accounts_list))

    def _have_all_aws_account_been_analyzed(self) -> bool:
        return (
            self._local_results.get_aws_account_index_to_analyze()
            == self._s3_uris_file_reader.get_number_of_aws_accounts()
        )


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _AwsAccountProcess(_Process):
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        aws_account = self._get_aws_account_to_analyze()
        print(f"The following AWS account will be analyzed: {aws_account}")
        self._exit_program_if_no_aws_credentials_in_terminal()
        if self._local_results.get_aws_account_index_to_analyze() == 0:
            self._local_results.create_analysis_results_folder()
        export_s3_data_of_account(aws_account)

    def _get_aws_account_to_analyze(self) -> str:
        aws_account_index_to_analyze = self._local_results.get_aws_account_index_to_analyze()
        aws_accounts_to_analyze = self._s3_uris_file_reader.get_aws_accounts()
        if aws_account_index_to_analyze >= len(aws_accounts_to_analyze):
            sys.exit("All AWS accounts have been analyzed")
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


# TODO drop
class _AwsAccountExportProcess(_Process):
    def __init__(self, aws_account: str):
        self._aws_account = aws_account

    def run(self):
        export_s3_data_of_account(self._aws_account)


class _AwsAccountsCombinationProcess(_Process):
    def run(self):
        export_s3_data_all_accounts_to_one_file()


class _AnalysisProcess(_Process):
    def run(self):
        S3DataAnalyzer().run()
        LocalResults().remove_file_with_analysis_date()


def _get_aws_account_process() -> _Process:
    return _AwsAccountProcess()


# TODO drop
def _get_aws_account_export_process(aws_account: str) -> _Process:
    return _AwsAccountExportProcess(aws_account)


def _get_aws_accounts_combination_process() -> _Process:
    return _AwsAccountsCombinationProcess()


def _get_analysis_process() -> _Process:
    return _AnalysisProcess()


if __name__ == "__main__":
    run()
