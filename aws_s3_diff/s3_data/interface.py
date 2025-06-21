from abc import ABC
from abc import abstractmethod

from aws_s3_diff.types_custom import Df


class MultiIndexDfCreator:
    @abstractmethod
    def get_df(self) -> Df:
        pass


class CsvExporter(ABC):
    @abstractmethod
    def export_df(self, df: Df):
        pass


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class DataGenerator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass
