import datetime
import os
import unittest
from pathlib import Path

from dateutil.tz import tzutc
from moto import mock_aws
from pandas import read_csv as read_csv_as_df
from pandas.testing import assert_frame_equal

from src import extract as m_extract
from src.config import Config
from tests.aws import S3
from tests.aws import set_aws_credentials
from tests.config import get_config_for_the_test
from tests.utils import remove_file_with_analysis_date_if_exists

ExpectedResult = list[dict]


class TestAwsAccountExtractor(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        S3().create_objects()
        remove_file_with_analysis_date_if_exists()

    def tearDown(self):
        self.mock_aws.stop()

    def test_run_using_config_generates_expected_result(self):
        config = get_config_for_the_test()
        run_using_config(config)
        result_df = read_csv_as_df(f"tests/s3-results/{config._folder_name_buckets_results}/aws_account_1_pro.csv")
        expected_result_df = read_csv_as_df("tests/s3-results/expected-results-test_extract/aws_account_1_pro.csv")
        expected_result_df["date"] = result_df["date"]
        assert_frame_equal(result_df, expected_result_df)


# TODO deprecate
def run_using_config(config: Config):
    # TODO use _LocalResults
    _create_folders_for_buckets_results(config)
    s3_queries = config.get_s3_queries()
    file_path_for_results = config.get_local_path_file_query_results()
    m_extract.AwsAccountExtractor(file_path_for_results, s3_queries).extract()


# TODO move it to _LocalResults
def _create_folders_for_buckets_results(config: Config):
    exported_files_directory_path = config.get_local_path_directory_bucket_results()
    print("Creating folder for bucket results: ", exported_files_directory_path)
    # TODO do it better
    if not Path(exported_files_directory_path).exists():
        os.makedirs(exported_files_directory_path)


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
        expected_result = [{"name": "cars-20241014.csv", "date": self._datetime_now, "size": 49}]
        s3_query = m_extract.S3Query("cars", "europe/spain/")
        self._test_get_s3_data_returns_expected_result(expected_result, s3_query)

    def test_get_s3_data_returns_expected_result_for_bucket_pets(self):
        expected_result = [
            {"name": "dogs-20241015.csv", "date": self._datetime_now, "size": 28},
            {"name": "dogs-20241019.csv", "date": self._datetime_now, "size": 20},
        ]
        s3_query = m_extract.S3Query("pets", "dogs/big_size/")
        self._test_get_s3_data_returns_expected_result(expected_result, s3_query)

    @property
    def _datetime_now(self) -> datetime.datetime:
        return datetime.datetime.now(tzutc())

    def _test_get_s3_data_returns_expected_result(self, expected_result: ExpectedResult, s3_query: m_extract.S3Query):
        s3_client = m_extract._S3Client()
        result = s3_client.get_s3_data(s3_query)
        self._test_get_s3_data_returns_expected_result_for_file_name(expected_result, result)

    def _test_get_s3_data_returns_expected_result_for_file_name(
        self, expected_result: ExpectedResult, result_to_check: list[dict]
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
