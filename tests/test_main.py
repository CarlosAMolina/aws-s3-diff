import shutil
import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

from pandas import DataFrame as Df
from pandas import read_csv
from pandas.testing import assert_frame_equal

from src import main as m_main
from src.local_results import _AnalysisPaths
from src.local_results import _MainPaths
from src.local_results import LocalResults
from src.s3_uris_to_analyze import S3UrisFileAnalyzer
from tests.aws import S3Server


class TestFunction_runLocalS3Server(unittest.TestCase):
    def setUp(self):
        """As the S3Server takes time to run, it is better to group all tests with local sever in only one test class"""
        self._local_s3_server = S3Server()
        self._local_s3_server.start()
        # Drop file created by the user
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    def tearDown(self):
        self._local_s3_server.stop()

    @patch(
        "src.main.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    @patch("src.main.input", create=True)
    def test_run(self, mock_input, mock_directory_path_what_to_analyze):
        mock_input.side_effect = ["Y"] * len(S3UrisFileAnalyzer().get_aws_accounts())
        for aws_account in S3UrisFileAnalyzer().get_aws_accounts():
            self._local_s3_server.create_objects(aws_account)
            m_main.run()
        analysis_paths = _AnalysisPaths(self._get_analysis_date_time_str())
        self._test_extracted_aws_accounts_data(analysis_paths)
        self._test_analysis_file(analysis_paths)
        shutil.rmtree(analysis_paths.directory_analysis)

    def _get_analysis_date_time_str(self) -> str:
        analysis_directory_names = [
            directory_path.name for directory_path in _MainPaths().directory_all_results.glob("20*")
        ]
        analysis_directory_names.sort()
        return analysis_directory_names[0]

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

    def _test_extracted_aws_accounts_data(self, analysis_paths: _AnalysisPaths):
        for aws_account, file_path_name_expected_result in {
            "aws_account_1_pro": "tests/fake-files/s3-results/20241201180132/aws_account_1_pro.csv",
            "aws_account_2_release": "tests/fake-files/s3-results/20241201180132/aws_account_2_release.csv",
            "aws_account_3_dev": "tests/fake-files/s3-results/20241201180132/aws_account_3_dev.csv",
        }.items():
            file_path_results = analysis_paths.directory_analysis.joinpath(f"{aws_account}.csv")
            result_df = read_csv(file_path_results)
            expected_result_df = read_csv(file_path_name_expected_result)
            expected_result_df["date"] = result_df["date"]
            assert_frame_equal(expected_result_df, result_df)

    def _test_analysis_file(self, analysis_paths: _AnalysisPaths):
        result = self._get_df_from_csv(analysis_paths.file_analysis)
        expected_result = self._get_df_from_csv_expected_result()
        date_column_names = ["aws_account_1_pro_date", "aws_account_2_release_date", "aws_account_3_dev_date"]
        assert_frame_equal(expected_result.drop(columns=date_column_names), result.drop(columns=date_column_names))
