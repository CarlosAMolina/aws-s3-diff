import unittest
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from unittest.mock import Mock

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from src.s3_data.all_accounts import AccountsDf
from src.s3_data.analysis import _AccountsToCompare
from src.s3_data.analysis import _AnalysisFromMultiSimpleIndexDfCreator
from src.s3_data.analysis import _AnalysisNewDfCreator
from src.s3_data.analysis import _CanFileExistTwoAccountsAnalysisFactory
from src.s3_data.analysis import _IsFileCopiedTwoAccountsAnalysisFactory
from src.s3_data.analysis import _TwoAccountsAnalysisFactory


class _AnalysisBuilderConfig(ABC):
    @property
    @abstractmethod
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisFactory]:
        pass

    @property
    @abstractmethod
    def file_name_and_expected_result(self) -> dict[str, list]:
        pass

    @property
    @abstractmethod
    def column_name_to_check(self) -> str:
        pass


class _IsFileCopiedAnalysisBuilderConfig(_AnalysisBuilderConfig):
    @property
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisFactory]:
        return _IsFileCopiedTwoAccountsAnalysisFactory

    @property
    def file_name_and_expected_result(self) -> dict[str, list]:
        return {
            "file-sync-ok.csv": [True],
            "file-sync-wrong.csv": [False],
            "file-not-in-origin.csv": [False],
            "file-not-in-target.csv": [False],
            "file-not-in-origin-target.csv": [True],
        }

    @property
    def column_name_to_check(self) -> str:
        return "is_sync_ok_in_release"


class _CanFileExistAnalysisBuilderConfig(_AnalysisBuilderConfig):
    @property
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisFactory]:
        return _CanFileExistTwoAccountsAnalysisFactory

    @property
    def file_name_and_expected_result(self) -> dict[str, list]:
        return {
            "file-sync-ok.csv": [None],
            "file-not-in-origin.csv": [False],
            "file-not-in-target.csv": [None],
            "file-not-in-origin-target.csv": [None],
        }

    @property
    def column_name_to_check(self) -> str:
        return "can_exist_in_release"


class TestDfAnalysis(unittest.TestCase):
    def test_get_df_set_analysis_result_for_several_df_analysis(self):
        for analysis_config in [
            _IsFileCopiedAnalysisBuilderConfig(),
            _CanFileExistAnalysisBuilderConfig(),
        ]:
            self._run_test_get_df_set_analysis_for_several_file_cases(analysis_config)

    def _run_test_get_df_set_analysis_for_several_file_cases(self, config: _AnalysisBuilderConfig):
        for file_name, expected_result in config.file_name_and_expected_result.items():
            file_path_name = f"fake-files/possible-s3-files-all-accounts/{file_name}"
            df = _get_df_from_accounts_s3_data_csv(file_path_name)
            result = config.analysis_class_to_check(_AccountsToCompare("pro", "release"), df).get_df_set_analysis()
            result_to_check = result.loc[:, ("analysis", config.column_name_to_check)].tolist()
            self.assertEqual(expected_result, result_to_check)


class TestAnalysisCsvFactory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    def test_get_df_set_analysis_columns(self):
        # TODO try to call _AnalysisNewDfCreator._get_df_s3_data_analyzed(df)
        file_path_name = "fake-files/test-full-analysis/s3-files-all-accounts.csv"
        df = _get_df_from_accounts_s3_data_csv(file_path_name)
        result = _AnalysisNewDfCreator()._get_df_set_analysis_columns(df)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = _AnalysisFromMultiSimpleIndexDfCreator(result).get_df()
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        expected_result = expected_result.astype(
            {"dev_date": str, "is_sync_ok_in_release": "object", "is_sync_ok_in_dev": "object"}
        )
        expected_result = expected_result.replace({"None": None})
        result_as_csv_export = result_as_csv_export.replace({np.nan: None})
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected-results", "analysis.csv")
        result = read_csv(expected_result_file_path).astype(
            {
                "pro_size": "Int64",
                "release_size": "Int64",
                "dev_size": "Int64",
            }
        )
        # https://stackoverflow.com/questions/26763344/convert-pandas-column-to-datetime/26763793#26763793
        date_column_names = ["pro_date", "release_date", "dev_date"]
        result[date_column_names] = result[date_column_names].apply(to_datetime)
        return result


def _get_df_from_accounts_s3_data_csv(file_path_name: str) -> Df:
    # TODO rename variable
    accounts_from_csv_df_factory = AccountsDf()
    accounts_from_csv_df_factory._accounts_simple_index_df_creator._get_file_path = lambda: (
        Path(__file__).parent.absolute().joinpath(file_path_name)
    )
    accounts_from_csv_df_factory._accounts_simple_index_df_creator._df_from_csv_creator._get_file_path = lambda: (
        Path(__file__).parent.absolute().joinpath(file_path_name)
    )
    accounts_from_csv_df_factory._accounts_simple_index_df_creator._df_from_csv_creator._s3_uris_file_reader = Mock()
    accounts_from_csv_df_factory._accounts_simple_index_df_creator._df_from_csv_creator._s3_uris_file_reader.get_accounts.return_value = _AccountsToCompare(
        "pro", "release"
    )
    return accounts_from_csv_df_factory.get_df()
