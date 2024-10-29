import os
import unittest
from collections import namedtuple
from pathlib import Path

import boto3
from moto import mock_aws

from src import extract as m_extract
from tests.config import get_config_for_the_test

FilePaths = namedtuple("FilePaths", "local_file_name_path, s3_file_name_path_name")


class TestS3Client(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    BUCKET_NAME_PETS = "pets"

    def setUp(self):
        self._set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        self.s3 = boto3.resource("s3")
        self.s3_client = boto3.client("s3")
        bucket = self.s3.Bucket(self.BUCKET_NAME_PETS)
        bucket.create()
        self.s3_dir_path_name_cars = "/pets/cars/europe/spain/"
        self.s3_dir_path_name_dogs = "/pets/dogs/big_size/"
        self.cars_file_name = "cars-20241014.csv"
        self.dogs_file_name = "dogs-20241019.csv"
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
        cars_file_paths = FilePaths(
            local_s3_files_path.joinpath("cars", "europe", "spain", self.cars_file_name),
            f"{self.s3_dir_path_name_cars}{self.cars_file_name}",
        )
        dogs_file_paths = FilePaths(
            local_s3_files_path.joinpath("pets", "dogs", "big_size", self.dogs_file_name),
            f"{self.s3_dir_path_name_dogs}{self.dogs_file_name}",
        )
        for file_paths in (cars_file_paths, dogs_file_paths):
            with open(file_paths.local_file_name_path, "rb") as data:
                self.s3_client.upload_fileobj(data, self.BUCKET_NAME_PETS, file_paths.s3_file_name_path_name)

    def tearDown(self):
        self.mock_aws.stop()

    def test_run_using_config_generates_expected_result(self):
        config = get_config_for_the_test()
        m_extract._run_using_config(config)
        # TODO check results
