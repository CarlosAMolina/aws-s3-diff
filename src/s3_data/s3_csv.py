from abc import ABC
from abc import abstractmethod
from pathlib import Path

from local_results import LocalResults
from logger import get_logger
from s3_data.all_accounts import AccountsAsSingleIndexFactory
from s3_data.all_accounts import AccountsNewDfFactory
from s3_data.analysis import AnalysisAsSingleIndexFactory
from s3_data.analysis import AnalysisNewDfFactory
from s3_data.one_account import AccountNewDfFactory
from types_custom import Df

# TODO implement these classes
# TODO replace all classes with the ones in this file
# TODO deprecate `account` argument, replace with _FileNameCreator and genereate it by checking the files
# TODO move the classes in this file to the one_account.py, all_accounts.py and analysis.py files


class _DfCreator(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass


class _SimpleIndexDfCreator(_DfCreator):
    # TODO add logic: return read_file() if file exists else create_df()
    pass


class _MultiIndexDfCreator(_DfCreator):
    # add logic: 1º get simple index, 2º convert index
    pass


class _AccountSimpleIndexDfCreator(_SimpleIndexDfCreator):
    def __init__(self, account: str):
        self._account = account

    def get_df(self) -> Df:
        # TODO deprecate, rename as _AccountSimpleIndexDfCreator
        return AccountNewDfFactory(self._account).get_df()


class _AccountsSimpleIndexDfCreator(_SimpleIndexDfCreator):
    def __init__(self):
        # TODO deprecate these classes, rename
        self._accounts_new_df_factory = AccountsNewDfFactory()
        self._accounts_as_single_index_factory = AccountsAsSingleIndexFactory()

    def get_df(self) -> Df:
        df = self._accounts_new_df_factory.get_df()
        return self._accounts_as_single_index_factory.get_df(df)


class _AnalysisSimpleIndexDfCreator(_SimpleIndexDfCreator):
    def __init__(self):
        self._analysis_new_df_factory = AnalysisNewDfFactory()
        self._analysis_as_single_index_factory = AnalysisAsSingleIndexFactory()
        self._logger = get_logger()

    def get_df(self) -> Df:
        df = self._analysis_new_df_factory.get_df()
        return self._analysis_as_single_index_factory.get_df(df)


# TODO
class _AccountMultiIndexDfCreator(_MultiIndexDfCreator):
    pass


# TODO
class _AccountsMultiIndexDfCreator(_MultiIndexDfCreator):
    pass


class _FileNameCreator(ABC):
    @abstractmethod
    def get_file_name(self) -> str:
        pass


class _AccountFileNameCreator(_FileNameCreator):
    # TODO move here the logic of
    # - read config file
    # - read results paths
    # - get 1º account in config file not in results path
    def __init__(self, account: str):
        self._account = account

    # TODO deprecate get_file_path_account_results, use this method instead
    def get_file_name(self) -> str:
        return f"{self._account}.csv"


class _AccountsFileNameCreator(_FileNameCreator):
    # TODO deprecate file_s3_data_all_accounts with this
    def get_file_name(self) -> str:
        return "s3-files-all-accounts.csv"


# TODO
class _AnalysisFileNameCreator(_FileNameCreator):
    # TODO deprecate file_analysis() with this
    def get_file_name(self) -> str:
        return "analysis.csv"


# TODO replace all CsvFactory with this class
class _CsvCreator(ABC):
    def __init__(self):
        self._local_results = LocalResults()
        self._logger = get_logger()

    def export_csv(self):
        df = self._get_df_creator().get_df()
        file_path = self._get_file_path()
        self._logger.info(f"Exporting {file_path}")
        df.to_csv(index=False, path_or_buf=file_path)

    @abstractmethod
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        pass

    def _get_file_path(self) -> Path:
        # TODO avoid access values of attribute of a class
        return self._local_results.analysis_paths.directory_analysis.joinpath(
            self._get_file_name_creator().get_file_name()
        )

    @abstractmethod
    def _get_file_name_creator(self) -> _FileNameCreator:
        pass


class AccountCsvCreator(_CsvCreator):
    def __init__(self, account: str):
        self._account = account
        super().__init__()

    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AccountSimpleIndexDfCreator(self._account)

    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AccountFileNameCreator(self._account)


class AccountsCsvCreator(_CsvCreator):
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AccountsSimpleIndexDfCreator()

    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AccountsFileNameCreator()


class AnalysisCsvCreator(_CsvCreator):
    def _get_df_creator(self) -> _SimpleIndexDfCreator:
        return _AnalysisSimpleIndexDfCreator()

    def _get_file_name_creator(self) -> _FileNameCreator:
        return _AnalysisFileNameCreator()
