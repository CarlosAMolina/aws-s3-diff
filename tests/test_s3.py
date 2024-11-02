import datetime
import unittest

from dateutil.tz import tzutc
from moto import mock_aws

from src import s3 as m_s3
from tests.aws import S3
from tests.aws import set_aws_credentials


class TestS3Client(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    def setUp(self):
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        S3().create_objects()

    def tearDown(self):
        self.mock_aws.stop()

    def test_get_s3_data_returns_expected_result_for_bucket_cars(self):
        file_name = "cars-20241014.csv"
        file_size = 49
        s3_query = m_s3.S3Query("cars", "europe/spain/")
        self._test_get_s3_data_returns_expected_result(file_name, file_size, s3_query)

    def test_get_s3_data_returns_expected_result_for_bucket_pets(self):
        file_name = "dogs-20241019.csv"
        file_size = 20
        s3_query = m_s3.S3Query("pets", "dogs/big_size/")
        self._test_get_s3_data_returns_expected_result(file_name, file_size, s3_query)

    def _test_get_s3_data_returns_expected_result(self, file_name: str, file_size: int, s3_query: m_s3.S3Query):
        s3_client = m_s3.S3Client()
        s3_data = s3_client.get_s3_data(s3_query)
        result = s3_data[0]
        self._test_get_s3_data_returns_expected_result_for_file_name(file_name, file_size, result)

    def _test_get_s3_data_returns_expected_result_for_file_name(
        self, file_name: str, file_size: int, result_to_check: dict
    ):
        for key, expected_value in {
            "name": file_name,
            "date": datetime.datetime.now(tzutc()),
            "size": file_size,
        }.items():
            if key == "date":
                self.assertEqual(expected_value.date(), result_to_check[key].date())
            else:
                self.assertEqual(expected_value, result_to_check[key])
