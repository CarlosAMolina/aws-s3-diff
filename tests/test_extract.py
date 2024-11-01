import unittest

import boto3
from moto import mock_aws
from pandas import read_csv as read_csv_as_df
from pandas.testing import assert_frame_equal

from src import extract as m_extract
from tests.aws import S3
from tests.aws import set_aws_credentials
from tests.config import get_config_for_the_test


class TestFunction_run_using_config(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3_client = boto3.client("s3")
        S3().create_objects()

    def tearDown(self):
        self.mock_aws.stop()

    def test_run_using_config_generates_expected_result(self):
        config = get_config_for_the_test()
        m_extract._run_using_config(config)
        result_df = read_csv_as_df(f"tests/s3-results/{config._folder_name_buckets_results}/pets/dogs-big_size.csv")
        expected_result_df = read_csv_as_df("tests/s3-results/expected-results-test_extract/pets/dogs-big_size.csv")
        expected_result_df["date"] = result_df["date"]
        assert_frame_equal(result_df, expected_result_df)
