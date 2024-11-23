import unittest
from unittest import mock

from moto import mock_aws

from src import main as m_main
from tests.aws import S3
from tests.aws import set_aws_credentials
from tests.utils import remove_file_with_analysis_date_if_exists


class TestFunction_run(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        S3().create_objects()
        remove_file_with_analysis_date_if_exists()

    def tearDown(self):
        self.mock_aws.stop()

    @mock.patch("src.main.input", create=True)
    def test_run(self, mock_input):
        mock_input.side_effect = ["Y"]
        m_main.run()
