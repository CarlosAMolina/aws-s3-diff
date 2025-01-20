from pathlib import Path


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
