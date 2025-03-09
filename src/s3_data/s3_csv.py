from abc import ABC
from abc import abstractmethod

from pathlib import Path
from types_custom import Df
from types_custom import MultiIndexDf

# TODO implement these classes
# TODO replace all classes with the ones in this file


class _SimpleIndexDfCreator(ABC):
    pass


class _AccountSimpleIndexDfCreator(_SimpleIndexDfCreator):
    pass


class _AccountsSimpleIndexDfCreator(_SimpleIndexDfCreator):
    pass


class _AnalysisSimpleIndexDfCreator(_SimpleIndexDfCreator):
    pass


class _FileNameCreator(ABC):
    pass


class _AccountFileNameCreator(_FileNameCreator):
    pass


class _AccountsFileNameCreator(_FileNameCreator):
    pass


class _AnalysisFileNameCreator(_FileNameCreator):
    pass


def _get_results_directory_path() -> Path:
    return Path("TODO")


# TODO replace all CsvFactory with this class
class _CsvCreator(ABC):
    def get_export_csv(self):
        df = self._get_df_creator().get_df()
        file_name = self._get_file_name_creator().get_file_name()
        dir_path = _get_results_directory_path().joinpath(file_name)
        df.to_csv(dir_path, index=False)

    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        pass

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        pass


class _AccountCsvCreator(_CsvCreator):
    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AccountSimpleIndexDfCreator()

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AccountFileNameCreator()


class _AccountsCsvCreator(_CsvCreator):
    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AccountsSimpleIndexDfCreator()

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AccountsFileNameCreator()


class _AnalysisCsvCreator(_CsvCreator):
    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AnalysisSimpleIndexDfCreator()

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AnalysisFileNameCreator()
