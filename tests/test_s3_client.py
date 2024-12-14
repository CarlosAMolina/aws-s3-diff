import datetime
import unittest
from pathlib import Path
from unittest import mock

from dateutil.tz import tzutc
from moto import mock_aws
from pandas import read_csv as read_csv_as_df
from pandas.testing import assert_frame_equal

from src import s3_client as m_s3_client
from src.local_results import LocalResults
from src.s3_uris_to_analyze import S3UrisFileReader
from tests.aws import S3
from tests.aws import set_aws_credentials

ExpectedResult = list[dict]


class TestAwsAccountExtractor(unittest.TestCase):
    def setUp(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self.mock_aws = mock_aws()
        # Drop file created by the user or by other tests.
        if LocalResults()._get_file_path_accounts_analysis_date_time().is_file():
            LocalResults().remove_file_with_analysis_date()

    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
    )
    def test_extract_generates_expected_result(self, mock_directory_path_what_to_analyze):
        LocalResults().create_analysis_results_folder()
        for aws_account, file_path_name_expected_result in {
            "aws_account_1_pro": "tests/fake-files/s3-results/20241201180132/aws_account_1_pro.csv",
            "aws_account_2_release": "tests/fake-files/s3-results/20241201180132/aws_account_2_release.csv",
            "aws_account_3_dev": "tests/fake-files/s3-results/20241201180132/aws_account_3_dev.csv",
        }.items():
            self.mock_aws.start()
            S3(aws_account=aws_account).create_objects()
            file_path_results = LocalResults().get_file_path_aws_account_results(aws_account)
            s3_queries = S3UrisFileReader().get_s3_queries_for_aws_account(aws_account)
            m_s3_client.AwsAccountExtractor(file_path_results, s3_queries).extract()
            result_df = read_csv_as_df(file_path_results)
            expected_result_df = read_csv_as_df(file_path_name_expected_result)
            expected_result_df["date"] = result_df["date"]
            assert_frame_equal(expected_result_df, result_df)
            self.mock_aws.stop()


class TestS3Client(unittest.TestCase):
    """http://docs.getmoto.org/en/latest/docs/getting_started.html"""

    def setUp(self):
        set_aws_credentials()
        self.mock_aws = mock_aws()
        self.mock_aws.start()
        S3(aws_account="aws_account_1_pro").create_objects()

    def tearDown(self):
        self.mock_aws.stop()

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

    def _test_get_s3_data_returns_expected_result(self, expected_result: ExpectedResult, s3_query: m_s3_client.S3Query):
        s3_client = m_s3_client._S3Client()
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
