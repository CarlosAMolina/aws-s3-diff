import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

import numpy as np
from numpy import array
from pandas import DataFrame as Df
from pandas import MultiIndex
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from local_results import LocalResults
from s3_data import get_df_s3_data_all_accounts
from src.analysis import _AnalysisDfToCsv
from src.analysis import _AnalysisGenerator
from src.analysis import _CompareAwsAccounts
from src.analysis import _OriginFileSyncDfAnalysis
from src.s3_uris_to_analyze import S3UrisFileReader


class TestOriginFileSyncDfAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.aws_accounts = _CompareAwsAccounts("account_1", "account_2")

    def test_get_df_set_analysis_result_if_file_sync_is_ok(self):
        df = Df(array([[1, 1]]))
        df.columns = MultiIndex.from_tuples([(self.aws_accounts.origin, "size"), (self.aws_accounts.target, "size")])
        result = _OriginFileSyncDfAnalysis(self.aws_accounts, df).get_df_set_analysis()
        expected_result = Df({"foo": [1], "bar": [1], "baz": [True]}).astype({"baz": "object"})
        expected_result.columns = MultiIndex.from_tuples(
            [
                (self.aws_accounts.origin, "size"),
                (self.aws_accounts.target, "size"),
                ("analysis", "is_sync_ok_in_account_2"),
            ]
        )
        assert_frame_equal(expected_result, result)

    def test_get_df_set_analysis_result_if_file_sync_is_wrong(self):
        df = Df(array([[1, 2]]))
        df.columns = MultiIndex.from_tuples([(self.aws_accounts.origin, "size"), (self.aws_accounts.target, "size")])
        result = _OriginFileSyncDfAnalysis(self.aws_accounts, df).get_df_set_analysis()
        expected_result = Df({"foo": [1], "bar": [2], "baz": [False]}).astype({"baz": "object"})
        expected_result.columns = MultiIndex.from_tuples(
            [
                (self.aws_accounts.origin, "size"),
                (self.aws_accounts.target, "size"),
                ("analysis", "is_sync_ok_in_account_2"),
            ]
        )
        assert_frame_equal(expected_result, result)

    def test_get_df_set_analysis_result_if_no_file_to_sync(self):
        df = Df(array([[None, 2]]))
        df.columns = MultiIndex.from_tuples([(self.aws_accounts.origin, "size"), (self.aws_accounts.target, "size")])
        result = _OriginFileSyncDfAnalysis(self.aws_accounts, df).get_df_set_analysis()
        expected_result = Df({"foo": [None], "bar": [2], "baz": ["No file to sync"]}).astype({"bar": "object"})
        expected_result.columns = MultiIndex.from_tuples(
            [
                (self.aws_accounts.origin, "size"),
                (self.aws_accounts.target, "size"),
                ("analysis", "is_sync_ok_in_account_2"),
            ]
        )
        assert_frame_equal(expected_result, result)

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-not-in-target.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_no_file_in_target_account(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        all_aws_accounts = S3UrisFileReader().get_aws_accounts()
        aws_accounts = _CompareAwsAccounts(*all_aws_accounts[:2])
        result = _OriginFileSyncDfAnalysis(aws_accounts, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [False]
        self.assertEqual(expected_result, result_to_check)


class TestAnalysisGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.current_path = Path(__file__).parent.absolute()

    @patch("src.analysis.LocalResults._get_analysis_date_time_str")
    @patch("src.analysis.LocalResults._get_path_directory_all_results")
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_get_df_s3_data_analyzed(
        self,
        mock_directory_path_what_to_analyze,
        mock_get_path_directory_all_results,
        mock_get_analysis_date_time_str,
    ):
        mock_get_path_directory_all_results.return_value = self.current_path.joinpath(
            "fake-files", LocalResults._FOLDER_NAME_S3_RESULTS
        )
        mock_get_analysis_date_time_str.return_value = "20241201180132"
        result = _AnalysisGenerator()._get_df_s3_data_analyzed()
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = (
            _AnalysisDfToCsv()
            ._get_df_to_export(result)
            .reset_index()
            .astype({"is_sync_ok_in_aws_account_2_release": "str", "is_sync_ok_in_aws_account_3_dev": "str"})
        )
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        result_as_csv_export = result_as_csv_export.replace({np.nan: None})
        assert_frame_equal(expected_result, result_as_csv_export)

    def _get_df_from_csv_expected_result(self) -> Df:
        expected_result_file_path = self.current_path.joinpath("expected-results", "analysis.csv")
        result = read_csv(expected_result_file_path).astype(
            {
                "aws_account_1_pro_size": "Int64",
                "aws_account_2_release_size": "Int64",
                "aws_account_3_dev_size": "Int64",
            }
        )
        # https://stackoverflow.com/questions/26763344/convert-pandas-column-to-datetime/26763793#26763793
        date_column_names = ["aws_account_1_pro_date", "aws_account_2_release_date", "aws_account_3_dev_date"]
        result[date_column_names] = result[date_column_names].apply(to_datetime)
        return result
