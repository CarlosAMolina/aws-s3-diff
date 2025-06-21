import datetime
from pathlib import Path

from aws_s3_diff.logger import get_logger

_EXTENSION_FILE_NAME = ".csv"
_ACCOUNTS_FILE_NAME = f"s3-files-all-accounts{_EXTENSION_FILE_NAME}"
_ANALYSIS_FILE_NAME = f"analysis{_EXTENSION_FILE_NAME}"


def get_account_file_name(account: str) -> str:
    return f"{account}{_EXTENSION_FILE_NAME}"


class LocalPaths:
    _current_path = Path(__file__).parent.absolute()

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

    def get_file_names_results(self) -> list[str]:
        paths = self.analysis_paths.directory_analysis.glob(f"*{_EXTENSION_FILE_NAME}")
        return [path.name for path in paths]

    def get_file_path_account(self, account: str) -> Path:
        return self.get_file_path_results(get_account_file_name(account))

    def get_file_path_results(self, file_name: str) -> Path:
        return self.analysis_paths.directory_analysis.joinpath(file_name)

    def get_file_path_all_accounts(self) -> Path:
        return self.get_file_path_results(_ACCOUNTS_FILE_NAME)

    def get_file_path_analysis(self) -> Path:
        return self.get_file_path_results(_ANALYSIS_FILE_NAME)

    def drop_file(self, file_path: Path):
        self._logger.debug(f"Removing: {file_path}")
        file_path.unlink()

    def drop_file_with_analysis_date(self):
        self.drop_file(self._analysis_date_time_file_path)

    def create_directory_analysis(self):
        self._logger.debug(f"Creating the directory: {self.analysis_paths.directory_analysis}")
        self.analysis_paths.directory_analysis.mkdir()

    def exist_analysis_date_time_file(self) -> bool:
        return self._analysis_date_time_file_path.is_file()

    def exist_directory_analysis(self) -> bool:
        return self.analysis_paths.directory_analysis.exists()

    @property
    def analysis_paths(self) -> "_AnalysisPaths":
        if self._analysis_paths_cache is None:
            # get_analysis_date_time_str has file input and outputs, don't do this in __init__.
            analysis_date_time_str = AnalysisDateTimeGenerator().get_analysis_date_time_str()
            self._analysis_paths_cache = _AnalysisPaths(analysis_date_time_str)
        return self._analysis_paths_cache


class AnalysisDateTimeGenerator:
    def __init__(self):
        self._analysis_date_time_file_path = LocalPaths().analysis_date_time_file

    def export_analysis_date_time_str(self):
        with open(self._analysis_date_time_file_path, "w") as file:
            date_time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file.write(date_time_str)

    def get_analysis_date_time_str(self) -> str:
        with open(self._analysis_date_time_file_path) as file:
            # `strip()` to avoid errors if the file is modified manually by te user.
            return file.read().strip()


# TODO deprecate
class _AnalysisPaths:
    def __init__(self, analysis_date_time_str: str):
        self._analysis_date_time_str = (
            analysis_date_time_str  # TODO use AnalysisDateTimeGenerator and drop __init__ argument
        )
        self._all_results_directory_path = LocalPaths().all_results_directory

    @property
    def directory_analysis(self) -> Path:
        return self._all_results_directory_path.joinpath(self._analysis_date_time_str)
