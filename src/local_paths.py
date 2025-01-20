from pathlib import Path


class LocalPaths:
    def __init__(self):
        self._current_path = Path(__file__).parent.absolute()

    @property
    def config_directory(self) -> Path:
        return Path(__file__).parent.parent.joinpath("config")

    @property
    def directory_all_results(self) -> Path:
        return self._current_path.parent.joinpath("s3-results")

    @property
    def file_analysis_date_time(self) -> Path:
        return self.directory_all_results.joinpath("analysis_date_time.txt")
