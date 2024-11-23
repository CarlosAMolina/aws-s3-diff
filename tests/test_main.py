import unittest
from pathlib import Path
from unittest import mock

from moto import mock_aws

from src import main as m_main
from src.local_results import LocalResults
from tests.aws import S3
from tests.aws import set_aws_credentials


class TestFunction_run(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        S3().create_objects()
        self._remove_file_with_analysis_date_if_exists()

    def _remove_file_with_analysis_date_if_exists(self):
        if Path(LocalResults._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).is_file():
            Path(LocalResults._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).unlink()

    def tearDown(self):
        self.mock_aws.stop()

    @mock.patch("src.main.input", create=True)
    def test_run(self, mock_input):
        mock_input.side_effect = ["Y"]
        m_main.run()
