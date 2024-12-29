import unittest
from pathlib import Path
from unittest.mock import patch
from unittest.mock import PropertyMock

from src.local_results import _MainPaths
from src.local_results import LocalResults
from tests.aws import S3Server
from tests.test_main import TestFunction_runLocalS3Server

ExpectedResult = list[dict]


class TestWithLocalS3Server(unittest.TestCase):
    def setUp(self):
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
        TestFunction_runLocalS3Server().run_test_run(
            mock_input, mock_directory_path_what_to_analyze, self._local_s3_server
        )
