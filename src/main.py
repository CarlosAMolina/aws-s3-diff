import datetime
import os
import sys
from pathlib import Path

from config import get_s3_uris_file_reader
from constants import FOLDER_NAME_S3_RESULTS

FilePathNamesToCompare = tuple[str, str, str]


def run():
    _IteractiveMenu().run()


class _IteractiveMenu:
    def __init__(self):
        self._s3_uris_file_reader = get_s3_uris_file_reader()
        self._local_results = _LocalResults()

    def run(self):
        print("Welcome to the AWS S3 Diff tool!")
        self._show_aws_accounts_to_analyze()
        if self._local_results.get_aws_account_index_to_analyze() == 0:
            self._local_results.create_analysis_results_folder_if_required(
                self._s3_uris_file_reader.get_number_of_aws_accounts()
            )
        self._analyze_aws_account(self._get_aws_account_to_analyze())

    def _analyze_aws_account(self, aws_account: str):
        print(f"The following AWS account will be analyzed: {aws_account}")
        self._exit_program_if_no_aws_credentials_in_terminal()
        print(f"Analyzing the account: {aws_account}")

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
            if len(user_input) == 0:
                return


class _LocalResults:
    # TODO remove this file when the 3º account has been analyzed.
    _FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME = "/tmp/aws_s3_diff_analysis_date_time.txt"

    def get_aws_account_index_to_analyze(self) -> int:
        if self._get_number_of_aws_accounts_analyzed() > 2:
            raise ValueError("All AWS accounts have been analyzed")
        return self._get_number_of_aws_accounts_analyzed()

    def create_analysis_results_folder_if_required(self, number_of_aws_accounts_to_analyze: int):
        if (
            self._get_number_of_aws_accounts_analyzed() == 0
            and not self._get_path_analysis_results().is_dir()
            or self._get_number_of_aws_accounts_analyzed() == number_of_aws_accounts_to_analyze
        ):
            self._create_analysis_results_folder()

    def _create_analysis_results_folder(self):
        print(f"Creating the results folder: {self._get_path_analysis_results()}")
        self._get_path_analysis_results().mkdir()

    def _get_number_of_aws_accounts_analyzed(self) -> int:
        return len(self._get_aws_accounts_analyzed())

    def _get_aws_accounts_analyzed(self) -> list[str]:
        if self._get_path_analysis_results().is_dir():
            # TODO use self._get_path_analysis_results().iterdir():
            return os.listdir(self._get_path_analysis_results())
        return []

    def _get_path_analysis_results(self) -> Path:
        current_path = Path(__file__).parent.absolute()
        return current_path.parent.joinpath(FOLDER_NAME_S3_RESULTS, self._get_analysis_date_time_str())

    def _get_analysis_date_time_str(self) -> str:
        if not Path(self._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).is_file():
            self._export_date_time_str()
        return self._get_date_time_str_stored()

    def _export_date_time_str(self):
        date_time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        with open(self._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME, "w") as file:
            file.write(date_time_str)

    def _get_date_time_str_stored(self) -> str:
        with open(self._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME) as file:
            return file.read()


if __name__ == "__main__":
    run()
