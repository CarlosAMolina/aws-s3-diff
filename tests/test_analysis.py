import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

import numpy as np
from pandas import DataFrame as Df
from pandas import read_csv
from pandas import to_datetime
from pandas.testing import assert_frame_equal

from s3_data import get_df_s3_data_all_accounts
from src.analysis import _AnalysisDfToCsv
from src.analysis import _AnalysisGenerator
from src.analysis import _CompareAwsAccounts
from src.analysis import _OriginFileSyncDfAnalysis
from src.analysis import _TargetAccountWithoutMoreFilesDfAnalysis
from src.s3_uris_to_analyze import S3UrisFileReader


class TestOriginFileSyncDfAnalysis(unittest.TestCase):
    # TODO refactor extract common patchs.
    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-sync-ok.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_file_sync_is_ok(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _OriginFileSyncDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [True]
        self.assertEqual(expected_result, result_to_check)

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-sync-wrong.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_file_sync_is_wrong(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _OriginFileSyncDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [False]
        self.assertEqual(expected_result, result_to_check)

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-not-in-origin.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_file_not_in_origin_but_in_target(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _OriginFileSyncDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [False]
        self.assertEqual(expected_result, result_to_check)

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
        result = _OriginFileSyncDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [False]
        self.assertEqual(expected_result, result_to_check)

    @property
    def _aws_accounts_to_compare(self) -> _CompareAwsAccounts:
        all_aws_accounts = S3UrisFileReader().get_aws_accounts()
        return _CompareAwsAccounts(*all_aws_accounts[:2])

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-not-in-origin-target.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_no_file_in_origin_target_account(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _OriginFileSyncDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "is_sync_ok_in_aws_account_2_release")].tolist()
        expected_result = [True]
        self.assertEqual(expected_result, result_to_check)


class TestTargetAccountWithoutMoreFilesAnalysisConfig(unittest.TestCase):
    # TODO refactor extract common patchs.
    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-sync-ok.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_file_sync(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _TargetAccountWithoutMoreFilesDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "can_exist_in_aws_account_2_release")].tolist()
        expected_result = [None]
        self.assertEqual(expected_result, result_to_check)

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-not-in-origin.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_no_file_to_sync(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _TargetAccountWithoutMoreFilesDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "can_exist_in_aws_account_2_release")].tolist()
        expected_result = [False]
        self.assertEqual(expected_result, result_to_check)

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
        result = _TargetAccountWithoutMoreFilesDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "can_exist_in_aws_account_2_release")].tolist()
        expected_result = [None]
        self.assertEqual(expected_result, result_to_check)

    @patch(
        "src.analysis.LocalResults.get_file_path_s3_data_all_accounts",
        return_value=Path(__file__)
        .parent.absolute()
        .joinpath("fake-files/test-origin-file-sync/s3-files-all-accounts/file-not-in-origin-target.csv"),
    )
    @patch(
        "src.analysis.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-origin-file-sync/"),
    )
    def test_get_df_set_analysis_result_if_no_file_in_origin_target_account(
        self, mock_directory_path_what_to_analyze, mock_get_file_path_s3_data_all_accounts
    ):
        df = get_df_s3_data_all_accounts()
        result = _TargetAccountWithoutMoreFilesDfAnalysis(self._aws_accounts_to_compare, df).get_df_set_analysis()
        result_to_check = result.loc[:, ("analysis", "can_exist_in_aws_account_2_release")].tolist()
        expected_result = [None]
        self.assertEqual(expected_result, result_to_check)

    @property
    def _aws_accounts_to_compare(self) -> _CompareAwsAccounts:
        all_aws_accounts = S3UrisFileReader().get_aws_accounts()
        return _CompareAwsAccounts(*all_aws_accounts[:2])


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
        mock_get_path_directory_all_results.return_value = self.current_path.joinpath("fake-files", "s3-results")
        mock_get_analysis_date_time_str.return_value = "20241201180132"
        result = _AnalysisGenerator()._get_df_s3_data_analyzed()
        # Required to convert to str because reading a csv column with bools and strings returns a str column.
        result_as_csv_export = _AnalysisDfToCsv()._get_df_to_export(result).reset_index()
        expected_result = self._get_df_from_csv_expected_result()
        expected_result = expected_result.replace({np.nan: None})
        expected_result = expected_result.astype(
            {"is_sync_ok_in_aws_account_2_release": "object", "is_sync_ok_in_aws_account_3_dev": "object"}
        )
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
