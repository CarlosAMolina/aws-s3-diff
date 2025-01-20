from pathlib import Path


# TODO add test (I think is not being tested)
class LocalPaths:
    @property
    def config_directory(self) -> Path:
        return Path(__file__).parent.parent.joinpath("config")
