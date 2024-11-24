import datetime
import os
from pathlib import Path

from constants import FOLDER_NAME_S3_RESULTS


class LocalResults:
    # TODO remove this file when the 3º account has been analyzed.
    _FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME = "/tmp/aws_s3_diff_analysis_date_time.txt"

    def get_aws_account_index_to_analyze(self) -> int:
        return self._get_number_of_aws_accounts_analyzed()

    def create_analysis_results_folder_if_required(self, number_of_aws_accounts_to_analyze: int):
        if (
            self._get_number_of_aws_accounts_analyzed() == 0
            and not self._get_path_analysis_results().is_dir()
            or self._get_number_of_aws_accounts_analyzed() == number_of_aws_accounts_to_analyze
        ):
            self._create_analysis_results_folder()

    def get_file_path_aws_account_results(self, aws_account: str):
        file_name = f"{aws_account}.csv"
        return self._get_path_analysis_results().joinpath(file_name)

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
        return self.path_directory_all_results().joinpath(self._get_analysis_date_time_str())

    # TODO? make private
    # TODO rename to get_path...
    def path_directory_all_results(self) -> Path:
        current_path = Path(__file__).parent.absolute()
        return current_path.parent.joinpath(FOLDER_NAME_S3_RESULTS)

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
