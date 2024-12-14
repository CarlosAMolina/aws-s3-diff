import datetime
from pathlib import Path


class LocalResults:
    _FILE_NAME_ACCOUNTS_ANALYSIS_DATE_TIME = "aws_s3_diff_analysis_date_time.txt"
    _FOLDER_NAME_S3_RESULTS = "s3-results"

    def get_aws_account_index_to_analyze(self) -> int:
        return self._get_number_of_aws_accounts_analyzed()

    def get_file_path_analysis_result(self):
        return self._get_path_analysis_results().joinpath("analysis.csv")

    def get_file_path_s3_data_all_accounts(self):
        return self._get_path_analysis_results().joinpath("s3-files-all-accounts.csv")

    def get_file_path_aws_account_results(self, aws_account: str):
        return self._get_path_analysis_results().joinpath(self._get_file_name_aws_account_results(aws_account))

    def remove_file_with_analysis_date(self):
        self._get_file_path_accounts_analysis_date_time().unlink()

    def create_analysis_results_folder(self):
        print(f"Creating the results folder: {self._get_path_analysis_results()}")
        self._get_path_analysis_results().mkdir()

    def _get_file_name_aws_account_results(self, aws_account: str):
        return f"{aws_account}.csv"

    def _get_number_of_aws_accounts_analyzed(self) -> int:
        return len(self._get_aws_accounts_analyzed())

    def _get_aws_accounts_analyzed(self) -> list[str]:
        if self._get_path_analysis_results().is_dir():
            return [file_path.stem for file_path in self._get_path_analysis_results().iterdir()]
        return []

    def _get_path_analysis_results(self) -> Path:
        return self._get_path_directory_all_results().joinpath(self._get_analysis_date_time_str())

    def _get_path_directory_all_results(self) -> Path:
        current_path = Path(__file__).parent.absolute()
        return current_path.parent.joinpath(self._FOLDER_NAME_S3_RESULTS)

    def _get_analysis_date_time_str(self) -> str:
        if not Path(self._get_file_path_accounts_analysis_date_time()).is_file():
            self._export_date_time_str()
        return self._get_date_time_str_stored()

    def _export_date_time_str(self):
        date_time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        with open(self._get_file_path_accounts_analysis_date_time(), "w") as file:
            file.write(date_time_str)

    def _get_date_time_str_stored(self) -> str:
        with open(self._get_file_path_accounts_analysis_date_time()) as file:
            return file.read()

    def _get_file_path_accounts_analysis_date_time(self) -> Path:
        return self._get_path_directory_all_results().joinpath(self._FILE_NAME_ACCOUNTS_ANALYSIS_DATE_TIME)
