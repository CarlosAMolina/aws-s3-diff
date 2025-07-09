import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
from pandas import DataFrame as Df
from pandas import read_csv
from pandas.testing import assert_frame_equal

from aws_s3_diff.aws_s3_diff import AnalysisConfigError
from aws_s3_diff.aws_s3_diff import EndpointConnectionError
from aws_s3_diff.aws_s3_diff import FolderInS3UriError
from aws_s3_diff.aws_s3_diff import Main
from aws_s3_diff.aws_s3_diff import S3UrisFileReader
from aws_s3_diff.local_results import LocalPaths
from aws_s3_diff.local_results import LocalResults
from tests.aws import S3
from tests.aws import S3Server


class TestMainWithLocalS3Server(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_MAX_KEYS"] = "2"  # To check that multiple request loops work ok.
        self._original_current_path = LocalPaths._current_path
        tmp_directory_path_name = self.enterContext(tempfile.TemporaryDirectory())
        LocalPaths._current_path = Path(tmp_directory_path_name).joinpath("aws_s3_diff")
        self._copy_files_to_temporal_folder(tmp_directory_path_name)

    def tearDown(self):
        os.environ.pop("AWS_MAX_KEYS")
        LocalPaths._current_path = self._original_current_path

    def test_run_all_acounts_generates_expected_results_if_queries_without_results(self):
        with S3Server() as _:
            # TODO manage only 2 accounts to get a faster test
            for account in ["pro", "release", "dev"]:
                s3 = S3(account)
                s3.create_buckets()
                # Buckets are created but no objetes are loaded.
                Main().run()
        folder_name_expected_results = "if-queries-without-results"
        self._asssert_created_csv_files_have_expected_values(folder_name_expected_results)

    def test_run_all_acounts_generates_expected_results(self):
        with S3Server() as local_s3_server:
            for account in S3UrisFileReader().get_accounts():
                local_s3_server.create_objects(account)
                Main().run()
        folder_name_expected_results = "if-queries-with-results"
        self._asssert_created_csv_files_have_expected_values(folder_name_expected_results)

    def _asssert_created_csv_files_have_expected_values(self, folder_name_expected_results: str):
        directory_analysis_path = LocalPaths().all_results_directory.joinpath(self._get_analysis_date_time_str())
        self._assert_extracted_accounts_data_have_expected_values(directory_analysis_path, folder_name_expected_results)
        local_results = LocalResults()
        local_results._directory_analysis_path_cache = directory_analysis_path
        self._assert_analysis_file_has_expected_values(folder_name_expected_results, local_results)

    def _copy_files_to_temporal_folder(self, tmp_directory_path_name: str):
        for folder_name in ["aws_s3_diff", "config", "s3-results"]:
            Path(tmp_directory_path_name).joinpath(folder_name).mkdir()
        for origin_path, final_path in [
            (
                Path(__file__).parent.parent.joinpath("config/analysis-config.json"),
                Path(tmp_directory_path_name).joinpath("config/analysis-config.json"),
            ),
            (
                Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis/s3-uris-to-analyze.csv"),
                Path(tmp_directory_path_name).joinpath("config/s3-uris-to-analyze.csv"),
            ),
        ]:
            shutil.copyfile(origin_path, final_path)

    def _get_analysis_date_time_str(self) -> str:
        analysis_directory_names = [
            directory_path.name for directory_path in LocalPaths().all_results_directory.glob("20*")
        ]
        analysis_directory_names.sort()
        return analysis_directory_names[-1]

    def _assert_extracted_accounts_data_have_expected_values(self, directory_analysis_path: Path, folder_name: str):
        for file_name_expected_result in [
            "pro.csv",
            "release.csv",
            "dev.csv",
        ]:
            file_path_results = directory_analysis_path.joinpath(file_name_expected_result)
            result_df = read_csv(file_path_results)
            expected_result_df = read_csv(f"tests/expected-results/{folder_name}/{file_name_expected_result}")
            expected_result_df["date"] = result_df["date"]
            assert_frame_equal(expected_result_df, result_df)

    def _assert_analysis_file_has_expected_values(self, folder_name: str, local_results: LocalResults):
        result = self._get_df_from_csv(local_results.get_file_path_analysis())
        expected_result = self._get_df_from_csv_expected_result(folder_name)
        date_column_names = ["pro_date", "release_date", "dev_date"]
        assert_frame_equal(expected_result.drop(columns=date_column_names), result.drop(columns=date_column_names))

    def _get_df_from_csv(self, path: Path) -> Df:
        return read_csv(path).astype(
            {
                "pro_size": "Int64",
                "release_size": "Int64",
                "dev_size": "Int64",
            }
        )

    def _get_df_from_csv_expected_result(self, folder_name: str) -> Df:
        current_path = Path(__file__).parent.absolute()
        expected_result_file_path = current_path.joinpath(f"expected-results/{folder_name}/analysis.csv")
        return self._get_df_from_csv(expected_result_file_path)


# TODO continue here
class TestMainWithoutLocalS3Server(unittest.TestCase):
    @patch("aws_s3_diff.aws_s3_diff._CsvsGenerator")
    def test_run_manages_analysis_config_error_and_generates_expected_error_messages(self, mock_s3_diff_process):
        mock_s3_diff_process().get_df.side_effect = AnalysisConfigError("foo")
        with self.assertLogs(level="ERROR") as cm:
            Main().run()
        self.assertEqual("foo", cm.records[0].message)

    @patch("aws_s3_diff.aws_s3_diff.LocalResults")
    @patch("aws_s3_diff.aws_s3_diff.have_all_accounts_been_analyzed")
    @patch("aws_s3_diff.aws_s3_diff.get_account_to_analyze")
    @patch("aws_s3_diff.aws_s3_diff.AccountDataGenerator.get_df")
    def test_run_manages_aws_client_errors_and_generates_expected_error_messages(
        self,
        mock_get_df,
        mock_get_account_to_analyze,
        mock_have_all_accounts_been_analyzed,
        mock_local_results,
    ):
        message_error_subfolder = (
            "Subfolders detected in bucket 'bucket-1'. The current version of the program cannot manage subfolders"
            ". Subfolders (1): folder/subfolder/"
        )
        for expected_error_message, aws_error in (
            (
                "Incorrect AWS credentials. Authenticate and run the program again",
                _ListObjectsV2ClientErrorBuilder().with_error_code("InvalidAccessKeyId").build(),
            ),
            (
                "Incorrect AWS credentials. Authenticate and run the program again",
                _ListObjectsV2ClientErrorBuilder().with_error_code("AccessDenied").build(),
            ),
            (
                "The bucket 'invented_bucket' does not exist. Specify a correct bucket and run the program again",
                _ListObjectsV2ClientErrorBuilder()
                .with_error_code("NoSuchBucket")
                .with_bucket_name("invented_bucket")
                .build(),
            ),
            (
                message_error_subfolder,
                FolderInS3UriError(message_error_subfolder),
            ),
            (
                'Could not connect to the endpoint URL: "foo"',
                EndpointConnectionError(endpoint_url="foo"),
            ),
        ):
            with self.subTest(expected_error_message=expected_error_message, aws_error=aws_error):
                mock_get_df.side_effect = aws_error
                self._mock_to_not_generate_analysis_date_time_file(
                    mock_get_account_to_analyze, mock_have_all_accounts_been_analyzed, mock_local_results
                )
                with self.assertLogs(level="ERROR") as cm:
                    Main().run()
                self.assertEqual(expected_error_message, cm.records[0].message)

    # TODO try avoid this method using temporal directories
    def _mock_to_not_generate_analysis_date_time_file(
        self, mock_get_account_to_analyze, mock_have_all_accounts_been_analyzed, mock_local_results
    ):
        mock_get_account_to_analyze.return_value = S3UrisFileReader().get_accounts()[0]
        mock_have_all_accounts_been_analyzed.return_value = False
        mock_local_results().analysis_paths.directory_analysis.is_dir.return_value = True
        mock_local_results().get_file_path_all_accounts().is_file.return_value = False
        mock_local_results().directory_analysis.is_dir.return_value = True


class _ListObjectsV2ClientErrorBuilder:
    def __init__(self):
        self._error_response = {"Error": {"Code": "foo", "BucketName": "foo"}}

    def with_bucket_name(self, bucket_name: str) -> "_ListObjectsV2ClientErrorBuilder":
        self._error_response["Error"]["BucketName"] = bucket_name
        return self

    def with_error_code(self, code: str) -> "_ListObjectsV2ClientErrorBuilder":
        self._error_response["Error"]["Code"] = code
        return self

    def build(self) -> ClientError:
        return ClientError(self._error_response, "ListObjectsV2")
