from abc import ABC
from abc import abstractmethod

from types_custom import Df
from types_custom import MultiIndexDf


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class _IndexModifier(ABC):
    @abstractmethod
    def get_df(self) -> Df | MultiIndexDf:
        pass


class AsMultiIndexModifier(_IndexModifier):
    @abstractmethod
    def get_df(self) -> MultiIndexDf:
        pass
