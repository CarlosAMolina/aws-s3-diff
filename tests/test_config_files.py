import unittest
from pathlib import Path
from unittest.mock import Mock
from unittest.mock import patch
from unittest.mock import PropertyMock

from aws_s3_diff import config_files as m_config_files
from aws_s3_diff.exceptions import AnalysisConfigError
from aws_s3_diff.exceptions import DuplicatedUriS3UrisFileError
from aws_s3_diff.exceptions import EmptyAccountNameS3UrisFileError
from aws_s3_diff.exceptions import EmptyUriS3UrisFileError
from aws_s3_diff.types_custom import S3Query


# TODO continue here
class TestAnalysisConfigChecker(unittest.TestCase):
    @patch("aws_s3_diff.config_files.AnalysisConfigReader")
    @patch("aws_s3_diff.config_files.S3UrisFileReader")
    def test_assert_file_is_correct_raises_expected_exception_if_origin_account_does_not_exist(
        self, mock_s3_uris_file_reader, mock_analysis_config_reader
    ):
        mock_analysis_config_reader.return_value.get_account_origin.return_value = "pr"
        mock_s3_uris_file_reader.return_value.get_accounts.return_value = ["pro", "release"]
        with self.assertRaises(AnalysisConfigError) as exception:
            m_config_files.AnalysisConfigChecker().assert_file_is_correct()
        self.assertEqual(
            "The AWS account 'pr' is defined in analysis-config.json but not in s3-uris-to-analyze.csv",
            str(exception.exception),
        )

    @patch("aws_s3_diff.config_files.AnalysisConfigReader")
    @patch("aws_s3_diff.config_files.S3UrisFileReader")
    def test_assert_file_is_correct_raises_expected_exception_if_target_account_does_not_exist(
        self, mock_s3_uris_file_reader, mock_analysis_config_reader
    ):
        for expected_result, accounts_target_config_file, accounts_target_uri_file in [
            [
                "The AWS account 'releas' is defined in analysis-config.json but not in s3-uris-to-analyze.csv",
                ["releas"],
                ["pro", "release"],
            ]
        ]:
            with self.subTest(
                expected_result=expected_result,
                accounts_target_config_file=accounts_target_config_file,
                accounts_target_uri_file=accounts_target_uri_file,
            ):
                mock_analysis_config_reader.return_value.get_account_origin.return_value = "pro"
                mock_analysis_config_reader.return_value.get_accounts_where_files_must_be_copied.return_value = (
                    accounts_target_config_file
                )
                mock_s3_uris_file_reader.return_value.get_accounts.return_value = accounts_target_uri_file
                with self.assertRaises(AnalysisConfigError) as exception:
                    m_config_files.AnalysisConfigChecker().assert_file_is_correct()
                self.assertEqual(expected_result, str(exception.exception))

    @patch("aws_s3_diff.config_files.AnalysisConfigReader")
    @patch("aws_s3_diff.config_files.S3UrisFileReader")
    def test_assert_file_is_correct_raises_expected_exception_messages(
        self, mock_s3_uris_file_reader, mock_analysis_config_reader
    ):
        accounts_target_config_file = ["releas", "de", "pre"]
        accounts_target_uri_file = ["pro", "release", "dev", "pre"]
        expected_result = (
            "The AWS accounts 'de', 'releas' are defined in analysis-config.json but not in s3-uris-to-analyze.csv"
        )
        mock_analysis_config_reader.return_value.get_account_origin.return_value = "pro"
        mock_analysis_config_reader.return_value.get_accounts_where_files_must_be_copied.return_value = (
            accounts_target_config_file
        )
        mock_s3_uris_file_reader.return_value.get_accounts.return_value = accounts_target_uri_file
        with self.assertRaises(AnalysisConfigError) as exception:
            m_config_files.AnalysisConfigChecker().assert_file_is_correct()
        self.assertEqual(
            expected_result,
            str(exception.exception),
        )


class TestS3UrisFileReader(unittest.TestCase):
    @patch(
        "aws_s3_diff.config_files.LocalPaths.config_directory",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis"),
    )
    def test_get_s3_queries_for_account_returns_if_accounts_with_different_buckets(self, mock_config_directory):
        for account, expected_result in {
            "pro": [
                S3Query("cars", "europe/spain"),
                S3Query("pets", "dogs/big_size"),
                S3Query("pets", "horses/europe"),
                S3Query("pets", "non-existent-prefix"),
            ],
            "release": [
                S3Query("cars", "europe/spain"),
                S3Query("pets", "dogs/big_size"),
                S3Query("pets", "horses/europe"),
                S3Query("pets", "non-existent-prefix"),
            ],
            "dev": [
                S3Query("cars_dev", "europe/spain"),
                S3Query("pets_dev", "dogs/size/heavy"),
                S3Query("pets_dev", "horses/europe"),
                S3Query("pets_dev", "non-existent-prefix"),
            ],
        }.items():
            result = m_config_files.S3UrisFileReader().get_s3_queries_for_account(account)
            self.assertEqual(expected_result, result)


class TestS3UrisFileChecker(unittest.TestCase):
    @patch("aws_s3_diff.config_files.LocalPaths.config_directory", new_callable=PropertyMock, return_value=Mock())
    def test_assert_file_is_correct_raises_exception_if_empty_account(self, mock_config_directory):
        mock_config_directory.return_value.joinpath.return_value = self._get_file_path_s3_uri_to_analyze(
            "empty_account.csv"
        )
        with self.assertRaises(EmptyAccountNameS3UrisFileError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Error in s3-uris-to-analyze.csv. Some AWS account names are empty", str(exception.exception))

    @patch("aws_s3_diff.config_files.LocalPaths.config_directory", new_callable=PropertyMock, return_value=Mock())
    def test_assert_file_is_correct_raises_exception_if_empty_uri(self, mock_config_directory):
        mock_config_directory.return_value.joinpath.return_value = self._get_file_path_s3_uri_to_analyze(
            "empty_uri.csv"
        )
        with self.assertRaises(EmptyUriS3UrisFileError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual("Error in s3-uris-to-analyze.csv. Some URIs are empty", str(exception.exception))

    @patch("aws_s3_diff.config_files.LocalPaths.config_directory", new_callable=PropertyMock, return_value=Mock())
    def test_assert_file_is_correct_raises_exception_if_duplicated_account(self, mock_config_directory):
        mock_config_directory.return_value.joinpath.return_value = self._get_file_path_s3_uri_to_analyze(
            "duplicated_uri.csv"
        )
        with self.assertRaises(DuplicatedUriS3UrisFileError) as exception:
            m_config_files.S3UrisFileChecker().assert_file_is_correct()
        self.assertEqual(
            "Error in s3-uris-to-analyze.csv. The AWS account foo has duplicated URIs", str(exception.exception)
        )

    @staticmethod
    def _get_file_path_s3_uri_to_analyze(file_name: str) -> Path:
        return Path(__file__).parent.absolute().joinpath("fake-files/s3-uris-to-analyze/possible-values", file_name)
