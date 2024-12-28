import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

from src.local_results import _MainPaths
from src.local_results import LocalResults
from tests import test_main as m_test_main
from tests import test_s3_client as m_test_s3_client
from tests import test_s3_data as m_test_s3_data
from tests.aws import S3Server

ExpectedResult = list[dict]


class TestWithLocalS3Server(unittest.TestCase):
    def setUp(self):
        # TODO rename in all files _s3_server to _mock_s3_server
        self._s3_server = S3Server()
        self._s3_server.start()
        # Drop file created by the user
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    def tearDown(self):
        self._s3_server.stop()

    @patch(
        "src.s3_uris_to_analyze.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    @patch("src.local_results._AnalysisDateTime._new_date_time_str", new_callable=PropertyMock)
    @patch("src.local_results._AnalysisDateTime.get_analysis_date_time_str")
    def test_run_test_s3_data(
        self,
        mock_get_analysis_date_time_str,
        mock_new_date_time_str,
        mock_directory_path_what_to_analyze,
    ):
        fake_analysis_date_time_str = "20240101000000-test-s3-data"
        mock_get_analysis_date_time_str.return_value = fake_analysis_date_time_str
        mock_new_date_time_str.return_value = fake_analysis_date_time_str
        m_test_s3_data.TestAwsAccountExtractor().run_test_extract_generates_expected_result(self._s3_server)

    def test_run_test_s3_client(self):
        self._s3_server.create_objects("aws_account_1_pro")
        m_test_s3_client.TestS3Client().run_test_get_s3_data_returns_expected_result_for_bucket_cars()
        m_test_s3_client.TestS3Client().run_test_get_s3_data_returns_expected_result_for_bucket_pets()

    @patch(
        "src.main.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    @patch("src.main.input", create=True)
    def test_run(self, mock_input, mock_directory_path_what_to_analyze):
        m_test_main.TestFunction_run().run_test_run(mock_input, mock_directory_path_what_to_analyze, self._s3_server)
