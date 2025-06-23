import datetime
from pathlib import Path

from aws_s3_diff.logger import get_logger

_EXTENSION_FILE_NAME = ".csv"
_ACCOUNTS_FILE_NAME = f"s3-files-all-accounts{_EXTENSION_FILE_NAME}"
_ANALYSIS_FILE_NAME = f"analysis{_EXTENSION_FILE_NAME}"

_logger = get_logger()


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
        self._local_paths = LocalPaths()
        self._directory_analysis_path_cache = None

    def create_directory_analysis(self):
        _logger.debug(f"Creating the directory: {self._directory_analysis_path}")
        self._directory_analysis_path.mkdir()

    def drop_file(self, file_path: Path):
        _logger.debug(f"Removing: {file_path}")
        file_path.unlink()

    def drop_file_with_analysis_date(self):
        self.drop_file(self._local_paths.analysis_date_time_file)

    def exist_analysis_date_time_file(self) -> bool:
        return self._local_paths.analysis_date_time_file.is_file()

    def exist_directory_analysis(self) -> bool:
        return self._directory_analysis_path.exists()

    def get_file_names_results(self) -> list[str]:
        paths = self._directory_analysis_path.glob(f"*{_EXTENSION_FILE_NAME}")
        return [path.name for path in paths]

    def get_file_path_account(self, account: str) -> Path:
        return self._get_file_path_results(get_account_file_name(account))

    def get_file_path_all_accounts(self) -> Path:
        return self._get_file_path_results(_ACCOUNTS_FILE_NAME)

    def get_file_path_analysis(self) -> Path:
        return self._get_file_path_results(_ANALYSIS_FILE_NAME)

    @property
    def _directory_analysis_path(self) -> Path:
        if self._directory_analysis_path_cache is None:
            with open(self._local_paths.analysis_date_time_file) as file:
                # `strip()` to avoid errors if the file is modified manually by te user.
                analysis_date_time_str = file.read().strip()
            self._directory_analysis_path_cache = self._local_paths.all_results_directory.joinpath(
                analysis_date_time_str
            )
        return self._directory_analysis_path_cache

    def _get_file_path_results(self, file_name: str) -> Path:
        return self._directory_analysis_path.joinpath(file_name)


class AnalysisDateTimeExporter:
    def __init__(self):
        self._analysis_date_time_file_path = LocalPaths().analysis_date_time_file

    def export_analysis_date_time_str(self):
        with open(self._analysis_date_time_file_path, "w") as file:
            date_time_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file.write(date_time_str)
