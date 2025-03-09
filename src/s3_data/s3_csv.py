from abc import ABC
from abc import abstractmethod
from pathlib import Path

from local_results import LocalResults
from logger import get_logger
from s3_data.one_account import AccountNewDfFactory
from types_custom import Df

# TODO implement these classes
# TODO replace all classes with the ones in this file
# TODO deprecate `account` argument, replace with _FileNameCreator and genereate it by checking the files


class _SimpleIndexDfCreator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class _AccountSimpleIndexDfCreator(_SimpleIndexDfCreator):
    def __init__(self, account: str):
        self._account = account

    def get_df(self) -> Df:
        # TODO deprecate, rename as _AccountSimpleIndexDfCreator
        return AccountNewDfFactory(self._account).get_df()


class _AccountsSimpleIndexDfCreator(_SimpleIndexDfCreator):
    pass


class _AnalysisSimpleIndexDfCreator(_SimpleIndexDfCreator):
    pass


class _FileNameCreator(ABC):
    @abstractmethod
    def get_file_name(self) -> str:
        pass


class _AccountFileNameCreator(_FileNameCreator):
    def __init__(self, account: str):
        self._account = account

    # TODO deprecate get_file_path_account_results, use this method instead
    def get_file_name(self) -> str:
        return f"{self._account}.csv"


class _AccountsFileNameCreator(_FileNameCreator):
    pass


class _AnalysisFileNameCreator(_FileNameCreator):
    pass


# TODO replace all CsvFactory with this class
class _CsvCreator(ABC):
    def __init__(self):
        self._local_results = LocalResults()
        self._logger = get_logger()

    def export_csv(self):
        df = self._get_df_creator().get_df()
        file_path = self._get_file_path()
        self._logger.info(f"Exporting {file_path}")
        try:
            df.to_csv(index=False, path_or_buf=file_path)
        except Exception as exception:
            self._drop_file()
            raise exception

    def _get_file_path(self) -> Path:
        # TODO avoid access values of attribute of a class
        return self._local_results.analysis_paths.directory_analysis.joinpath(
            self._get_file_name_creator().get_file_name()
        )

    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        pass

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        pass

    # TODO check if drop an unexistent file raises an excepction, add try-except in that case
    def _drop_file(self):
        self._get_file_path().unlink()


class AccountCsvCreator(_CsvCreator):
    def __init__(self, account: str):
        self._account = account
        super().__init__()

    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AccountSimpleIndexDfCreator(self._account)

    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AccountFileNameCreator(self._account)

    def _get_file_path(self) -> Path:
        return self._local_results.get_file_path_account_results(self._account)


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
