from abc import ABC
from abc import abstractmethod
from pathlib import Path

from local_results import LocalResults
from logger import get_logger
from types_custom import Df
from types_custom import MultiIndexDf


class DfCreator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class NewDfCreator(DfCreator):
    pass


class FromCsvDfCreator(DfCreator):
    pass


class FileNameCreator(ABC):
    @abstractmethod
    def get_file_name(self) -> str:
        pass


class SimpleIndexDfCreator(DfCreator):
    # TODO add logic: return read_file() if file exists else create_df()
    pass


# TODO
class MultiIndexDfCreator(DfCreator):
    # add logic: 1º get simple index, 2º convert index
    pass


# TODO replace all CsvFactory with this class
class CsvCreator(ABC):
    def __init__(self):
        self._local_results = LocalResults()
        self._logger = get_logger()

    def export_csv(self):
        df = self._get_df_creator().get_df()
        file_path = self._get_file_path()
        self._logger.info(f"Exporting {file_path}")
        df.to_csv(index=False, path_or_buf=file_path)

    @abstractmethod
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        pass

    def _get_file_path(self) -> Path:
        # TODO avoid access values of attribute of a class
        return self._local_results.analysis_paths.directory_analysis.joinpath(
            self._get_file_name_creator().get_file_name()
        )

    @abstractmethod
    def _get_file_name_creator(self) -> FileNameCreator:
        pass


# TODO? deprecate
class IndexFactory(ABC):
    @abstractmethod
    def get_df(self, df: Df) -> Df | MultiIndexDf:
        pass


# TODO rename in all files all `factory` to creator
# TODO add return read_file if file.exists else creat_df()
class FromMultiSimpleIndexDfCreator(IndexFactory):
    @abstractmethod
    def get_df(self, df: Df) -> Df:
        pass

    def _set_df_columns_as_single_index(self, df: Df):
        df.columns = df.columns.map("_".join)
