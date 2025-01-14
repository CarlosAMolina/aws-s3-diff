import unittest
from pathlib import Path
from unittest import mock

from src import config_files as m_config_files
from types_custom import S3Query


class TestS3UriParts(unittest.TestCase):
    def test_bucket(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config_files._S3UriParts(uri).bucket
        self.assertEqual("my-bucket", result)

    def test_key(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config_files._S3UriParts(uri).key
        self.assertEqual("my-folder/my-object.png", result)


class TestS3UrisFileReader(unittest.TestCase):
    @mock.patch(
        "src.config_files.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis"),
    )
    def test_get_s3_queries_for_aws_account_returns_if_accounts_with_different_buckets(
        self, mock_directory_path_what_to_analyze
    ):
        for aws_account, expected_result in {
            "aws_account_1_pro": [
                S3Query("cars", "europe/spain"),
                S3Query("pets", "dogs/big_size"),
                S3Query("pets", "horses/europe"),
                S3Query("pets", "non-existent-prefix"),
            ],
            "aws_account_2_release": [
                S3Query("cars", "europe/spain"),
                S3Query("pets", "dogs/big_size"),
                S3Query("pets", "horses/europe"),
                S3Query("pets", "non-existent-prefix"),
            ],
            "aws_account_3_dev": [
                S3Query("cars_dev", "europe/spain"),
                S3Query("pets_dev", "dogs/size/heavy"),
                S3Query("pets_dev", "horses/europe"),
                S3Query("pets_dev", "non-existent-prefix"),
            ],
        }.items():
            result = m_config_files.S3UrisFileReader().get_s3_queries_for_aws_account(aws_account)
            self.assertEqual(expected_result, result)


class TestS3UrisFileChecker(unittest.TestCase):
    @mock.patch("src.config_files.S3UrisFileReader._file_path_what_to_analyze", new_callable=mock.PropertyMock)
    def test_assert_file_is_correct_raises_exception_if_empty_aws_account(self, mock_file_path_what_to_analyze):
        mock_file_path_what_to_analyze.return_value = self._get_file_path_s3_uri_to_analyze("empty_aws_account.csv")
        with self.assertRaises(ValueError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Some AWS account names are empty", str(exception.exception))

    @mock.patch("src.config_files.S3UrisFileReader._file_path_what_to_analyze", new_callable=mock.PropertyMock)
    def test_assert_file_is_correct_raises_exception_if_empty_uri(self, mock_file_path_what_to_analyze):
        mock_file_path_what_to_analyze.return_value = self._get_file_path_s3_uri_to_analyze("empty_uri.csv")
        with self.assertRaises(ValueError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Some URIs are empty", str(exception.exception))

    @mock.patch("src.config_files.S3UrisFileReader._file_path_what_to_analyze", new_callable=mock.PropertyMock)
    def test_assert_file_is_correct_raises_exception_if_duplicated_aws_account(self, mock_file_path_what_to_analyze):
        mock_file_path_what_to_analyze.return_value = self._get_file_path_s3_uri_to_analyze("duplicated_uri.csv")
        with self.assertRaises(ValueError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("The AWS account foo has duplicated URIs", str(exception.exception))

    @staticmethod
    def _get_file_path_s3_uri_to_analyze(file_name: str) -> Path:
        return Path(__file__).parent.absolute().joinpath("fake-files/s3-uris-to-analyze/possible-values", file_name)
