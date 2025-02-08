from abc import ABC
from abc import abstractmethod

from types_custom import Df
from types_custom import MultiIndexDf


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class _IndexFactory(ABC):
    @abstractmethod
    def get_df(self, df: Df) -> Df | MultiIndexDf:
        pass


class AsMultiIndexFactory(_IndexFactory):
    @abstractmethod
    def get_df(self, df: Df) -> MultiIndexDf:
        pass
