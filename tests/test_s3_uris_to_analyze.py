import unittest
from pathlib import Path
from unittest import mock

from src import s3_uris_to_analyze as m_uris_to_analyze
from types_custom import S3Query


class TestS3UriParts(unittest.TestCase):
    def test_bucket(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_uris_to_analyze._S3UriParts(uri).bucket
        self.assertEqual("my-bucket", result)

    def test_key(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_uris_to_analyze._S3UriParts(uri).key
        self.assertEqual("my-folder/my-object.png", result)


class TestS3UrisFileReader(unittest.TestCase):
    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._directory_path_what_to_analyze",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files"),
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
            result = m_uris_to_analyze.S3UrisFileReader().get_s3_queries_for_aws_account(aws_account)
            self.assertEqual(expected_result, result)


class TestS3UrisFileChecker(unittest.TestCase):
    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._file_what_to_analyze_path",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/s3-uris-to-analyze/empty_aws_account.csv"),
    )
    def test_assert_file_is_correct_raises_exception_if_empty_aws_account(self, mock_file_what_to_analyze_path):
        with self.assertRaises(ValueError) as exception:
            m_uris_to_analyze.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Some AWS account names are empty", str(exception.exception))

    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._file_what_to_analyze_path",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/s3-uris-to-analyze/empty_uri.csv"),
    )
    def test_assert_file_is_correct_raises_exception_if_empty_uri(self, mock_file_what_to_analyze_path):
        with self.assertRaises(ValueError) as exception:
            m_uris_to_analyze.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Some URIs are empty", str(exception.exception))

    @mock.patch(
        "src.s3_uris_to_analyze.S3UrisFileReader._file_what_to_analyze_path",
        new_callable=mock.PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/s3-uris-to-analyze/duplicated_uri.csv"),
    )
    def test_assert_file_is_correct_raises_exception_if_duplicated_aws_account(self, mock_file_what_to_analyze_path):
        with self.assertRaises(ValueError) as exception:
            m_uris_to_analyze.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("The AWS account foo has duplicated URIs", str(exception.exception))
