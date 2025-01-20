import datetime
from pathlib import Path

from local_paths import LocalPaths
from logger import get_logger


class LocalResults:
    def __init__(self):
        self._logger = get_logger()
        self._analysis_date_time_file_path = LocalPaths().analysis_date_time_file
        # TODO _get_analysis_date_time_str has file input and outputs, don't do this in __init__
        analysis_date_time_str = _AnalysisDateTime().get_analysis_date_time_str()
        self.analysis_paths = _AnalysisPaths(analysis_date_time_str)

    def has_this_aws_account_been_analyzed(self, aws_account: str) -> bool:
        return self.get_file_path_aws_account_results(aws_account).is_file()

    def get_file_path_aws_account_results(self, aws_account: str):
        return self.analysis_paths.directory_analysis.joinpath(f"{aws_account}.csv")

    def drop_file_with_analysis_date(self):
        self._logger.debug(f"Removing the file: {self._analysis_date_time_file_path}")
        self._analysis_date_time_file_path.unlink()

    def create_directory_analysis(self):
        self._logger.debug(f"Creating the directory: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.mkdir()

    def drop_directory_analysis(self):
        self._logger.debug(f"Removing the directory: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.rmdir()


class _AnalysisDateTime:
    def __init__(self):
        self._analysis_date_time_file_path = LocalPaths().analysis_date_time_file

    def get_analysis_date_time_str(self) -> str:
        if not self._analysis_date_time_file_path.is_file():
            self._export_date_time_str()
        return self._get_date_time_str_stored()

    def _export_date_time_str(self):
        with open(self._analysis_date_time_file_path, "w") as file:
            file.write(self._new_date_time_str)

    @property
    def _new_date_time_str(self) -> str:
        return datetime.datetime.now().strftime(self._date_time_format)

    @property
    def _date_time_format(self) -> str:
        return "%Y%m%d%H%M%S"

    def _get_date_time_str_stored(self) -> str:
        with open(self._analysis_date_time_file_path) as file:
            # `strip()` to avoid errors if the file is modified manually by te user.
            return file.read().strip()


class _AnalysisPaths:
    def __init__(self, analysis_date_time_str: str):
        self._analysis_date_time_str = analysis_date_time_str
        self._all_results_directory_path = LocalPaths().all_results_directory

    @property
    def directory_analysis(self) -> Path:
        return self._all_results_directory_path.joinpath(self._analysis_date_time_str)

    @property
    def file_analysis(self) -> Path:
        return self.directory_analysis.joinpath("analysis.csv")

    @property
    def file_s3_data_all_accounts(self) -> Path:
        return self.directory_analysis.joinpath("s3-files-all-accounts.csv")
