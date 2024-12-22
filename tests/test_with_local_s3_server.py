import unittest
from pathlib import Path
from unittest import mock

from src.local_results import _MainPaths
from src.local_results import LocalResults
from tests.aws import S3Server
from tests.test_s3_client import TestS3Client
from tests.test_s3_data import TestAwsAccountExtractor

ExpectedResult = list[dict]


class TestWithLocalS3Server(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._s3_server = S3Server()
        cls._s3_server.start()
        # Drop file created by the user or by other tests.
        if _MainPaths().file_analysis_date_time.is_file():
            LocalResults().remove_file_with_analysis_date()

    @classmethod
    def tearDownClass(cls):
        cls._s3_server.stop()

    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileAnalyzer._directory_path_what_to_analyze",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_run_test_of_TestAwsAccountExtractor(self, mock_directory_path_what_to_analyze):
        TestAwsAccountExtractor().run_test_extract_generates_expected_result(
            mock_directory_path_what_to_analyze, self._s3_server
        )

    def test_run_test_of_TestS3Client(self):
        # TODO remove previous created objects?
        self._s3_server.create_objects("aws_account_1_pro")
        TestS3Client().run_test_get_s3_data_returns_expected_result_for_bucket_cars()
        TestS3Client().run_test_get_s3_data_returns_expected_result_for_bucket_pets()
