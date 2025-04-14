import datetime
from pathlib import Path

from logger import get_logger

ACCOUNTS_FILE_NAME = "s3-files-all-accounts.csv"
ANALYSIS_FILE_NAME = "analysis.md"


def get_account_file_name(account: str) -> str:
    return f"{account}.csv"


class LocalPaths:
    def __init__(self):
        self._current_path = Path(__file__).parent.absolute()

    @property
    def config_directory(self) -> Path:
        return self._current_path.parent.joinpath("config")

    @property
    def all_results_directory(self) -> Path:
        return self._current_path.parent.joinpath("s3-results")

    @property
    def analysis_date_time_file(self) -> Path:
        return self.all_results_directory.joinpath("analysis_date_time.txt")


class LocalResults:
    def __init__(self):
        self._logger = get_logger()
        self._analysis_date_time_file_path = LocalPaths().analysis_date_time_file
        self._analysis_paths_cache = None  # To avoid read/create file in __init__.

    def has_this_account_been_analyzed(self, account: str) -> bool:
        return self._get_file_path_account_results(account).is_file()

    def get_file_path_results(self, file_name: str):
        return self.analysis_paths.directory_analysis.joinpath(file_name)

    def _get_file_path_account_results(self, account: str):
        return self.get_file_path_results(get_account_file_name(account))

    def drop_file_with_analysis_date(self):
        self._logger.debug(f"Removing the file: {self._analysis_date_time_file_path}")
        self._analysis_date_time_file_path.unlink()

    def create_directory_analysis(self):
        self._logger.debug(f"Creating the directory: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.mkdir()

    def drop_directory_analysis(self):
        self._logger.debug(f"Removing the directory: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.rmdir()

    @property
    def analysis_paths(self) -> "_AnalysisPaths":
        if self._analysis_paths_cache is None:
            # get_analysis_date_time_str has file input and outputs, don't do this in __init__.
            analysis_date_time_str = _AnalysisDateTimeCreator().get_analysis_date_time_str()
            self._analysis_paths_cache = _AnalysisPaths(analysis_date_time_str)
        return self._analysis_paths_cache


class _AnalysisDateTimeCreator:
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
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

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
        return self._all_results_directory_path.joinpath(self._get_analysis_date_time_str())

    @property
    def file_analysis(self) -> Path:
        return self.directory_analysis.joinpath(ANALYSIS_FILE_NAME)

    @property
    def file_s3_data_all_accounts(self) -> Path:
        return self.directory_analysis.joinpath(ACCOUNTS_FILE_NAME)

    def _get_analysis_date_time_str(self) -> str:
        # TODO use _AnalysisDateTimeCreator and drop __init__ argument
        return self._analysis_date_time_str
