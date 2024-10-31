import os
import unittest
from pathlib import Path

import boto3
from moto import mock_aws
from pandas import read_csv as read_csv_as_df
from pandas.testing import assert_frame_equal

from src import extract as m_extract
from tests.config import get_config_for_the_test


class TestS3Client(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    BUCKET_NAME = "pets"

    def setUp(self):
        self._set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3")
        bucket = self.s3.Bucket(self.BUCKET_NAME)
        bucket.create()
        self._upload_files()

    def _set_aws_credentials(self):
        """ "
        http://docs.getmoto.org/en/latest/docs/getting_started.html#how-do-i-avoid-tests-from-mutating-my-real-infrastructure
        """
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    def _upload_files(self):
        current_path = Path(__file__).parent.absolute()
        local_s3_files_path = current_path.joinpath("s3-files")
        s3_file_path_name = "dogs/big_size/dogs-20241019.csv"
        local_file_path = local_s3_files_path.joinpath(self.BUCKET_NAME, s3_file_path_name)
        with open(local_file_path, "rb") as data:
            self.s3_client.upload_fileobj(data, self.BUCKET_NAME, s3_file_path_name)

    def tearDown(self):
        self.mock_aws.stop()

    def test_run_using_config_generates_expected_result(self):
        config = get_config_for_the_test()
        m_extract._run_using_config(config)
        result_df = read_csv_as_df(f"tests/s3-results/{config._folder_name_buckets_results}/pets/dogs-big_size.csv")
        expected_result_df = read_csv_as_df("tests/s3-results/expected-results-test_extract/pets/dogs-big_size.csv")
        expected_result_df["date"] = result_df["date"]
        assert_frame_equal(result_df, expected_result_df)
