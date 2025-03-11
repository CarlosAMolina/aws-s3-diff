from logger import get_logger
from s3_data.all_accounts import AccountsAsSingleIndexFactory
from s3_data.all_accounts import AccountsNewDfFactory
from s3_data.analysis import AnalysisAsSingleIndexFactory
from s3_data.analysis import AnalysisNewDfFactory
from s3_data.interface import CsvCreator
from s3_data.interface import FileNameCreator
from s3_data.interface import MultiIndexDfCreator
from s3_data.interface import SimpleIndexDfCreator
from types_custom import Df

# TODO implement these classes
# TODO replace all classes with the ones in this file
# TODO deprecate `account` argument, replace with FileNameCreator and genereate it by checking the files
# TODO move the classes in this file to the one_account.py, all_accounts.py and analysis.py files


class _AccountsSimpleIndexDfCreator(SimpleIndexDfCreator):
    def __init__(self):
        # TODO deprecate these classes, rename
        self._accounts_new_df_factory = AccountsNewDfFactory()
        self._accounts_as_single_index_factory = AccountsAsSingleIndexFactory()

    def get_df(self) -> Df:
        df = self._accounts_new_df_factory.get_df()
        return self._accounts_as_single_index_factory.get_df(df)


class _AnalysisSimpleIndexDfCreator(SimpleIndexDfCreator):
    def __init__(self):
        self._analysis_new_df_factory = AnalysisNewDfFactory()
        self._analysis_as_single_index_factory = AnalysisAsSingleIndexFactory()
        self._logger = get_logger()

    def get_df(self) -> Df:
        df = self._analysis_new_df_factory.get_df()
        return self._analysis_as_single_index_factory.get_df(df)


# TODO
class _AccountMultiIndexDfCreator(MultiIndexDfCreator):
    pass


# TODO
class _AccountsMultiIndexDfCreator(MultiIndexDfCreator):
    pass


class _AccountsFileNameCreator(FileNameCreator):
    # TODO deprecate file_s3_data_all_accounts with this
    def get_file_name(self) -> str:
        return "s3-files-all-accounts.csv"


# TODO
class _AnalysisFileNameCreator(FileNameCreator):
    # TODO deprecate file_analysis() with this
    def get_file_name(self) -> str:
        return "analysis.csv"


class AccountsCsvCreator(CsvCreator):
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        return _AccountsSimpleIndexDfCreator()

    def _get_file_name_creator(self) -> FileNameCreator:
        return _AccountsFileNameCreator()


class AnalysisCsvCreator(CsvCreator):
    def _get_df_creator(self) -> SimpleIndexDfCreator:
        return _AnalysisSimpleIndexDfCreator()

    def _get_file_name_creator(self) -> FileNameCreator:
        return _AnalysisFileNameCreator()
