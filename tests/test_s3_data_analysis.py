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

from aws_s3_diff.s3_data.all_accounts import AccountsCsvReader
from aws_s3_diff.s3_data.analysis import _AccountsToCompare
from aws_s3_diff.s3_data.analysis import _CanFileExistTwoAccountsAnalysisSetter
from aws_s3_diff.s3_data.analysis import _IsFileCopiedTwoAccountsAnalysisSetter
from aws_s3_diff.s3_data.analysis import _TwoAccountsAnalysisSetter
from aws_s3_diff.s3_data.analysis import AnalysisDataGenerator


# TODO continue here
class _AnalysisConfig(ABC):
    @property
    @abstractmethod
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisSetter]:
        pass

    @property
    @abstractmethod
    def file_name_and_expected_result(self) -> dict[str, list]:
        pass

    @property
    @abstractmethod
    def column_name_to_check(self) -> str:
        pass


class _IsFileCopiedAnalysisConfig(_AnalysisConfig):
    @property
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisSetter]:
        return _IsFileCopiedTwoAccountsAnalysisSetter

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


class _CanFileExistAnalysisConfig(_AnalysisConfig):
    @property
    def analysis_class_to_check(self) -> type[_TwoAccountsAnalysisSetter]:
        return _CanFileExistTwoAccountsAnalysisSetter

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
        analysis_config_a = _IsFileCopiedAnalysisConfig()
        analysis_config_b = _CanFileExistAnalysisConfig()
        for file_name_and_expected_result, analysis_class_to_check, column_name_to_check in [
            [
                analysis_config_a.file_name_and_expected_result,
                analysis_config_a.analysis_class_to_check,
                analysis_config_a.column_name_to_check,
            ],
            [
                analysis_config_b.file_name_and_expected_result,
                analysis_config_b.analysis_class_to_check,
                analysis_config_b.column_name_to_check,
            ],
        ]:
            with self.subTest(
                file_name_and_expected_result=file_name_and_expected_result,
                analysis_class_to_check=analysis_class_to_check,
                column_name_to_check=column_name_to_check,
            ):
                for file_name, expected_result in file_name_and_expected_result.items():
                    file_path_name = f"fake-files/possible-s3-files-all-accounts/{file_name}"
                    df = self._get_df_accounts_csv(file_path_name)
                    result = analysis_class_to_check(
                        _AccountsToCompare("pro", "release"), df
                    ).get_df_set_analysis_column()
                    result_to_check = result.loc[:, ("analysis", column_name_to_check)].tolist()
                    self.assertEqual(expected_result, result_to_check)

    def _get_df_accounts_csv(self, file_path_name: str) -> Df:
        accounts_csv_reader = AccountsCsvReader()
        accounts_csv_reader._local_results = Mock()
        accounts_csv_reader._local_results.get_file_path_all_accounts.return_value = (
            Path(__file__).parent.absolute().joinpath(file_path_name)
        )
        accounts_csv_reader._s3_uris_file_reader = Mock()
        accounts_csv_reader._s3_uris_file_reader.get_accounts.return_value = _AccountsToCompare("pro", "release")
        return accounts_csv_reader.get_df()


class TestAnalysisCsvExporter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    def test_get_df_set_analysis_columns(self):
        # TODO try to call AnalysisDataGenerator._get_df_s3_data_analyzed(df)
        file_path_name = "fake-files/test-full-analysis/s3-files-all-accounts.csv"
        df = self._get_df_from_accounts_s3_data_csv(file_path_name)
        analysis_data_generator = AnalysisDataGenerator()
        result = analysis_data_generator._get_df_set_analysis_columns(df)
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = analysis_data_generator._get_df_with_single_index(result)
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        expected_result = expected_result.astype({"is_sync_ok_in_release": "object", "is_sync_ok_in_dev": "object"})
        expected_result = expected_result.replace({"None": None})
        result_as_csv_export = result_as_csv_export.replace({np.nan: None})
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_df_from_accounts_s3_data_csv(self, file_path_name: str) -> Df:
        accounts_csv_reader = AccountsCsvReader()
        accounts_csv_reader._local_results = Mock()
        accounts_csv_reader._local_results.get_file_path_all_accounts = lambda: (
            Path(__file__).parent.absolute().joinpath(file_path_name)
        )
        return accounts_csv_reader.get_df()

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected-results/if-queries-with-results/analysis.csv")
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
