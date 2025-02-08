from abc import ABC
from abc import abstractmethod

from types_custom import Df


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass
