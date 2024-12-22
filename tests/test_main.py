import shutil
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

from pandas import DataFrame as Df
from pandas import read_csv
from pandas.testing import assert_frame_equal

from src import main as m_main
from src.local_results import _MainPaths
from src.local_results import LocalResults
from src.s3_uris_to_analyze import S3UrisFileAnalyzer
from tests.aws import S3Server


class TestFunction_run(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        # TODO rename in all files _s3_server to _mock_s3_server
        cls._s3_server = S3Server()
        cls._s3_server.start()
        # Drop file created by the user
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    def tearDown(self):
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    @classmethod
    def tearDownClass(cls):
        cls._s3_server.stop()

    @patch(
        "src.analysis.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    @patch("src.main.input", create=True)
    @patch("src.main.LocalResults.remove_file_with_analysis_date")  # TODO RM
    def test_run(self, mock_remove_file_with_analysis_date, mock_input, mock_directory_path_what_to_analyze):
        self._run_test_run(
            mock_remove_file_with_analysis_date, mock_input, mock_directory_path_what_to_analyze, self._s3_server
        )

    def _run_test_run(
        self, mock_remove_file_with_analysis_date, mock_input, mock_directory_path_what_to_analyze, s3_server
    ):
        mock_input.side_effect = ["Y"] * len(S3UrisFileAnalyzer().get_aws_accounts())
        for aws_account in S3UrisFileAnalyzer().get_aws_accounts():
            s3_server.create_objects(aws_account)
            m_main.run()
        result = self._get_df_from_csv(LocalResults().analysis_paths.file_analysis)
        expected_result = self._get_df_from_csv_expected_result()
        date_column_names = ["aws_account_1_pro_date", "aws_account_2_release_date", "aws_account_3_dev_date"]
        assert_frame_equal(expected_result.drop(columns=date_column_names), result.drop(columns=date_column_names))
        mock_remove_file_with_analysis_date.assert_called_once()
        shutil.rmtree(LocalResults().analysis_paths.directory_analysis)

    def _get_df_from_csv_expected_result(self) -> Df:
        current_path = Path(__file__).parent.absolute()
        expected_result_file_path = current_path.joinpath("expected-results", "analysis.csv")
        return self._get_df_from_csv(expected_result_file_path)

    def _get_df_from_csv(self, path: Path) -> Df:
        return read_csv(path).astype(
            {
                "aws_account_1_pro_size": "Int64",
                "aws_account_2_release_size": "Int64",
                "aws_account_3_dev_size": "Int64",
            }
        )
