from abc import ABC
from abc import abstractmethod
from pathlib import Path

from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
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


class CsvGenerator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class CsvReader(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


# TODO deprecate
class CsvCreator(ABC):
    def __init__(self):
        self._local_results = LocalResults()
        self._logger = get_logger()

    def get_df(self) -> Df:
        return self._get_df_creator().get_df()

    def export_csv(self, df: Df):
        self._logger.info(f"Exporting {self.get_file_path()}")
        df.to_csv(index=False, path_or_buf=self.get_file_path())

    def get_file_path(self) -> Path:
        return self._local_results.get_file_path_results(self._file_name)

    @abstractmethod
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        pass

    @property
    @abstractmethod
    def _file_name(self) -> str:
        pass
