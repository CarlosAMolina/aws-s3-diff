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
        # Drop created file when the user runs the main program instead of the tests.
        if Path(LocalResults()._get_file_path_accounts_analysis_date_time()).is_dir():
            LocalResults().remove_file_with_analysis_date()

    def tearDown(self):
        self.mock_aws.stop()
        LocalResults().remove_file_with_analysis_date()

    @mock.patch("src.main.input", create=True)
    def test_run(self, mock_input):
        number_of_accounts_to_analyze = 3
        mock_input.side_effect = ["Y"] * number_of_accounts_to_analyze
        for _ in range(number_of_accounts_to_analyze):
            m_main.run()
