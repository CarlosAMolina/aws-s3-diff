import datetime
import os
import pathlib
import unittest

import boto3
from dateutil.tz import tzutc
from moto import mock_aws

from src import extract as m_extract


class TestFuntion_get_s3_data(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    BUCKET_NAME = "test-bucket"

    def setUp(self):
        self._set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3")
        bucket = self.s3.Bucket(self.BUCKET_NAME)
        bucket.create()
        # TODO rename add _cars
        self.s3_dir_path_name = "/pets/cars/europe/spain/"
        self.s3_dir_path_name_dogs = "/pets/dogs/big_size/"
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
        current_path = pathlib.Path(__file__).parent.absolute()
        s3_files_path = current_path.joinpath("s3-files")
        cars_file_path = s3_files_path.joinpath("cars", "europe", "spain", "cars-20241014.csv")
        dogs_file_name = "dogs-20241019.csv"
        dogs_file_path = s3_files_path.joinpath("pets", "dogs", "big_size", dogs_file_name)
        # TODO s3_file_path_name must be a name with `cars`
        with open(cars_file_path, "rb") as data:
            s3_file_path_name = f"{self.s3_dir_path_name}big-dogs.csv"
            self.s3_client.upload_fileobj(data, self.BUCKET_NAME, s3_file_path_name)
        with open(dogs_file_path, "rb") as data:
            s3_file_path_name = f"{self.s3_dir_path_name_dogs}dogs_file_name.csv"
            self.s3_client.upload_fileobj(data, self.BUCKET_NAME, s3_file_path_name)

    def tearDown(self):
        self.mock_aws.stop()

    def test_get_s3_data_returns_expected_result(self):
        s3_query = m_extract.S3Query(self.BUCKET_NAME, self.s3_dir_path_name)
        s3_data = m_extract._get_s3_data(s3_query)
        result_cars = s3_data[0]
        for key, expected_value in {
            "name": "big-dogs.csv",
            "date": datetime.datetime.now(tzutc()),
            "size": 49,
        }.items():
            if key == "date":
                self.assertEqual(expected_value.date(), result_cars[key].date())
            else:
                self.assertEqual(expected_value, result_cars[key])
