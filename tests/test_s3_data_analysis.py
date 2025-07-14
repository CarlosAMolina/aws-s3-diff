import unittest
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
from aws_s3_diff.s3_data.analysis import AnalysisDataGenerator


class TestDfAnalysis(unittest.TestCase):
    def test_get_df_set_analysis_result_for_several_df_analysis(self):
        for file_name_and_expected_result, analysis_class_to_check, column_name_to_check in [
            [
                {
                    "file-sync-ok.csv": [True],
                    "file-sync-wrong.csv": [False],
                    "file-not-in-origin.csv": [False],
                    "file-not-in-target.csv": [False],
                    "file-not-in-origin-target.csv": [True],
                },
                _IsFileCopiedTwoAccountsAnalysisSetter,
                "is_sync_ok_in_release",
            ],
            [
                {
                    "file-sync-ok.csv": [None],
                    "file-not-in-origin.csv": [False],
                    "file-not-in-target.csv": [None],
                    "file-not-in-origin-target.csv": [None],
                },
                _CanFileExistTwoAccountsAnalysisSetter,
                "can_exist_in_release",
            ],
        ]:
            with self.subTest(
                file_name_and_expected_result=file_name_and_expected_result,
                analysis_class_to_check=analysis_class_to_check,
                column_name_to_check=column_name_to_check,
            ):
                for file_name, expected_result in file_name_and_expected_result.items():
                    file_path_name = f"fake-files/possible-s3-files-all-accounts/{file_name}"
                    accounts_csv_reader = AccountsCsvReader()
                    accounts_csv_reader._local_results = Mock()
                    accounts_csv_reader._s3_uris_file_reader = Mock()
                    accounts_csv_reader._s3_uris_file_reader.get_accounts.return_value = _AccountsToCompare(
                        "pro", "release"
                    )
                    accounts_csv_reader._local_results.get_file_path_all_accounts.return_value = (
                        Path(__file__).parent.absolute().joinpath(file_path_name)
                    )
                    df = accounts_csv_reader.get_df()
                    result = analysis_class_to_check(
                        _AccountsToCompare("pro", "release"), df
                    ).get_df_set_analysis_column()
                    result_to_check = result.loc[:, ("analysis", column_name_to_check)].tolist()
                    self.assertEqual(expected_result, result_to_check)


# TODO continue here
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
