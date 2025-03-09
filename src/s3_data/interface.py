from abc import ABC
from abc import abstractmethod

from types_custom import Df
from types_custom import MultiIndexDf


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class CsvFactory(ABC):
    @abstractmethod
    def to_csv(self):
        pass


class _DfFactory(ABC):
    @abstractmethod
    def get_df(self) -> Df | MultiIndexDf:
        pass


class NewDfFactory(_DfFactory):
    pass


class FromCsvDfFactory(_DfFactory):
    @abstractmethod
    def get_df(self) -> MultiIndexDf:
        pass


class IndexFactory(ABC):
    @abstractmethod
    def get_df(self, df: Df) -> Df | MultiIndexDf:
        pass


class AsMultiIndexFactory(IndexFactory):
    @abstractmethod
    def get_df(self, df: Df) -> MultiIndexDf:
        pass


# TODO rename to SimpleIndexDfCreator and add:
# return read_file if file.exists else creat_df()
class AsSingleIndexFactory(IndexFactory):
    @abstractmethod
    def get_df(self, df: Df) -> Df:
        pass

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)
