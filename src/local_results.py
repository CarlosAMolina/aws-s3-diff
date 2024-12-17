import datetime
from pathlib import Path


class LocalResults:
    def __init__(self):
        self._main_paths = _MainPaths()

    def get_aws_account_index_to_analyze(self) -> int:
        return self._get_number_of_aws_accounts_analyzed()

    def get_file_path_analysis_result(self):
        return _AnalysisPaths(self._get_analysis_date_time_str()).file_analysis

    def get_file_path_s3_data_all_accounts(self):
        return self._get_path_analysis_results().joinpath("s3-files-all-accounts.csv")

    def get_file_path_aws_account_results(self, aws_account: str):
        return self._get_path_analysis_results().joinpath(f"{aws_account}.csv")

    def remove_file_with_analysis_date(self):
        self._main_paths.file_analysis_date_time.unlink()

    def create_analysis_results_folder(self):
        print(f"Creating the results folder: {self._get_path_analysis_results()}")
        self._get_path_analysis_results().mkdir()

    def _get_number_of_aws_accounts_analyzed(self) -> int:
        return len(self._get_aws_accounts_analyzed())

    def _get_aws_accounts_analyzed(self) -> list[str]:
        if self._get_path_analysis_results().is_dir():
            return [file_path.stem for file_path in self._get_path_analysis_results().iterdir()]
        return []

    def _get_path_analysis_results(self) -> Path:
        return _AnalysisPaths(self._get_analysis_date_time_str()).directory_analysis

    def _get_analysis_date_time_str(self) -> str:
        if not self._main_paths.file_analysis_date_time.is_file():
            self._export_date_time_str()
        return self._get_date_time_str_stored()

    def _export_date_time_str(self):
        date_time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        with open(self._main_paths.file_analysis_date_time, "w") as file:
            file.write(date_time_str)

    def _get_date_time_str_stored(self) -> str:
        with open(self._main_paths.file_analysis_date_time) as file:
            return file.read()


class _MainPaths:
    def __init__(self):
        self._current_path = Path(__file__).parent.absolute()

    @property
    def directory_all_results(self) -> Path:
        return self._current_path.parent.joinpath("s3-results")

    @property
    def file_analysis_date_time(self) -> Path:
        return self.directory_all_results.joinpath("analysis_date_time.txt")


class _AnalysisPaths:
    def __init__(self, analysis_date_time_str: str):
        self._analysis_date_time_str = analysis_date_time_str
        self._main_paths = _MainPaths()

    @property
    def directory_analysis(self) -> Path:
        return self._main_paths.directory_all_results.joinpath(self._analysis_date_time_str)

    @property
    def file_analysis(self):
        return self.directory_analysis.joinpath("analysis.csv")
