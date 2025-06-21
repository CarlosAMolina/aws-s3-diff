from abc import ABC
from abc import abstractmethod

from pandas import DataFrame as Df


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


class DfModifier(ABC):
    @abstractmethod
    def get_df_modified(self, df: Df) -> Df:
        pass
