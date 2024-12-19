import datetime
import unittest

from dateutil.tz import tzutc

from src import s3_client as m_s3_client
from tests.aws import S3Server

_ExpectedResult = list[dict]


class TestS3Client(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    @classmethod
    def setUpClass(cls):
        cls._s3_server = S3Server()
        cls._s3_server.start()
        cls._s3_server.create_objects("aws_account_1_pro")

    @classmethod
    def tearDownClass(cls):
        cls._s3_server.stop()

    def test_get_s3_data_returns_expected_result_for_bucket_cars(self):
        expected_result = [{"name": "cars-20241014.csv", "date": self._datetime_now, "size": 49}]
        s3_query = m_s3_client.S3Query("cars", "europe/spain/")
        self._test_get_s3_data_returns_expected_result(expected_result, s3_query)

    def test_get_s3_data_returns_expected_result_for_bucket_pets(self):
        expected_result = [
            {"name": "dogs-20241015.csv", "date": self._datetime_now, "size": 28},
            {"name": "dogs-20241019.csv", "date": self._datetime_now, "size": 20},
        ]
        s3_query = m_s3_client.S3Query("pets", "dogs/big_size/")
        self._test_get_s3_data_returns_expected_result(expected_result, s3_query)

    @property
    def _datetime_now(self) -> datetime.datetime:
        return datetime.datetime.now(tzutc())

    def _test_get_s3_data_returns_expected_result(
        self, expected_result: _ExpectedResult, s3_query: m_s3_client.S3Query
    ):
        s3_client = m_s3_client.S3Client()
        result = s3_client.get_s3_data(s3_query)
        self._test_get_s3_data_returns_expected_result_for_file_name(expected_result, result)

    def _test_get_s3_data_returns_expected_result_for_file_name(
        self, expected_result: _ExpectedResult, result_to_check: list[dict]
    ):
        for file_expected_result in expected_result:
            # https://stackoverflow.com/questions/8653516/search-a-list-of-dictionaries-in-python
            file_result_to_check = next(
                (file_result for file_result in result_to_check if file_result["name"] == file_expected_result["name"]),
                None,
            )
            assert file_result_to_check is not None
            self.assertEqual(file_expected_result["name"], file_result_to_check["name"])
            # No compare hour, minutes and seconds to avoid differences
            self.assertEqual(file_expected_result["date"].date(), file_result_to_check["date"].date())
            self.assertEqual(file_expected_result["size"], file_result_to_check["size"])
