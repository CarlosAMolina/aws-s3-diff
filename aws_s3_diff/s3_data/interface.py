from abc import ABC
from abc import abstractmethod

from aws_s3_diff.types_custom import Df


class DfCreator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class NewDfCreator(DfCreator):
    pass


class FromCsvDfCreator(DfCreator):
    pass


class SimpleIndexDfCreator(DfCreator):
    # TODO add logic: return read_file() if file exists else create_df()
    pass


# TODO
class MultiIndexDfCreator(DfCreator):
    # add logic: 1º get simple index, 2º convert index
    pass


# TODO add return read_file if file.exists else creat_df()
class FromMultiSimpleIndexDfCreator(DfCreator):
    def __init__(self, df: Df):
        self._df = df

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)


class FromSimpleMultiIndexDfCreator(DfCreator):
    def __init__(self, df: Df):
        self._df = df


class CsvExporter(ABC):
    @abstractmethod
    def export_df(self, df: Df):
        pass


class DataGenerator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass
