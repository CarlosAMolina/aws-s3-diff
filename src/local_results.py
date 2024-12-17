import datetime
from pathlib import Path


class LocalResults:
    def __init__(self):
        self._main_paths = _MainPaths()
        # TODO _get_analysis_date_time_str has file input and outputs, don't do this in __init__
        analysis_date_time_str = _AnalysisDateTime().get_analysis_date_time_str()
        self.analysis_paths = _AnalysisPaths(analysis_date_time_str)

    def get_aws_account_index_to_analyze(self) -> int:
        aws_accounts_analyzed = (
            [file_path.stem for file_path in self.analysis_paths.directory_analysis.iterdir()]
            if self.analysis_paths.directory_analysis.is_dir()
            else []
        )
        return len(aws_accounts_analyzed)

    def get_file_path_s3_data_all_accounts(self):
        return self.analysis_paths.file_s3_data_all_accounts

    def get_file_path_aws_account_results(self, aws_account: str):
        return self.analysis_paths.directory_analysis.joinpath(f"{aws_account}.csv")

    def remove_file_with_analysis_date(self):
        self._main_paths.file_analysis_date_time.unlink()

    def create_analysis_results_folder(self):
        print(f"Creating the results folder: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.mkdir()


class _AnalysisDateTime:
    def __init__(self):
        self._main_paths = _MainPaths()

    def get_analysis_date_time_str(self) -> str:
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

    @property
    def file_s3_data_all_accounts(self):
        return self.directory_analysis.joinpath("s3-files-all-accounts.csv")
