from pathlib import Path


class LocalPaths:
    @property
    def config_directory(self) -> Path:
        return Path(__file__).parent.parent.joinpath("config")
