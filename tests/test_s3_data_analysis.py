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
from aws_s3_diff.s3_data.analysis import _IsHashMatchedTwoAccountsAnalysisSetter
from aws_s3_diff.s3_data.analysis import AnalysisDataGenerator


class TestDfAnalysis(unittest.TestCase):
    def test_get_df_set_analysis_result_for_several_df_analysis(self):
        accounts_csv_reader = AccountsCsvReader()
        accounts_csv_reader._local_results = Mock()
        accounts_csv_reader._s3_uris_file_reader = Mock()
        accounts_csv_reader._s3_uris_file_reader.get_accounts.return_value = _AccountsToCompare("pro", "release")
        for file_name_and_expected_result, analysis_class_to_check, column_name_to_check in [
            [
                {
                    "file-sync-ok.csv": [True],
                    "file-sync-wrong.csv": [False],
                    "file-not-in-origin.csv": [False],
                    "file-not-in-target.csv": [False],
                    "file-not-in-origin-target.csv": [True],
                },
                _IsHashMatchedTwoAccountsAnalysisSetter,
                "is_hash_the_same_in_release",
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
                    accounts_csv_reader._local_results.get_file_path_all_accounts.return_value = (
                        Path(__file__).parent.absolute().joinpath(file_path_name)
                    )
                    df = accounts_csv_reader.get_df()
                    result = analysis_class_to_check(
                        _AccountsToCompare("pro", "release"), df
                    ).get_df_set_analysis_column()
                    result_to_check = result.loc[:, ("analysis", column_name_to_check)].tolist()
                    self.assertEqual(expected_result, result_to_check)


class TestAnalysisDataGenerator(unittest.TestCase):
    def test_get_df_returns_expected_result(self):
        mock_local_results = Mock()
        mock_local_results.get_file_path_all_accounts.return_value = (
            Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis/s3-files-all-accounts.csv")
        )
        analysis_data_generator = AnalysisDataGenerator()
        analysis_data_generator._accounts_csv_reader._local_results = mock_local_results
        result = analysis_data_generator.get_df()
        expected_result = self._get_df_expected_result_from_csv()
        result = result.replace({np.nan: None})
        assert_frame_equal(expected_result, result)

    def _get_df_expected_result_from_csv(self) -> Df:
        expected_result_file_path = (
            Path(__file__).parent.absolute().joinpath("expected-results/if-queries-with-results/analysis.csv")
        )
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
        result = result.replace({np.nan: None})
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result = result.astype({"is_hash_the_same_in_release": "object", "is_hash_the_same_in_dev": "object"})
        return result.replace({"None": None})
