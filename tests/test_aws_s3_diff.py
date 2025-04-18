import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from unittest.mock import PropertyMock

from botocore.exceptions import ClientError
from pandas import DataFrame as Df
from pandas import read_csv
from pandas.testing import assert_frame_equal

from aws_s3_diff.aws_s3_diff import AnalysisConfigError
from aws_s3_diff.aws_s3_diff import FolderInS3UriError
from aws_s3_diff.aws_s3_diff import Main
from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.local_results import _AnalysisPaths
from aws_s3_diff.local_results import LocalPaths
from aws_s3_diff.local_results import LocalResults
from tests.aws import S3Server


class TestMainWithLocalS3Server(unittest.TestCase):
    def setUp(self):
        self._local_s3_server = S3Server()
        # Drop file created by the user
        if LocalPaths().analysis_date_time_file.is_file():
            LocalResults().drop_file_with_analysis_date()
        os.environ["AWS_MAX_KEYS"] = "2"  # To check that multiple request loops work ok.

    def tearDown(self):
        os.environ.pop("AWS_MAX_KEYS")

    # TODO rename drop new
    # TODO uncomment
    def test_run_if_should_work_ok_new(self):
        # TODO refactor all lines in this function
        main_project_path = Path(__file__).parent.parent
        with TemporaryDirectory() as tmp_directory_path_name:
            # TODO copy fake test config  to tmp
            print("Created temporary directory", tmp_directory_path_name)  # TODO rm
            tmp_directory_path = Path(tmp_directory_path_name)
            sys.path.append(str(tmp_directory_path))
            sys.path.append(str(tmp_directory_path.joinpath("aws_s3_diff")))
            for folder_name in ["config", "s3-results", "aws_s3_diff"]:
                shutil.copytree(
                    main_project_path.joinpath(folder_name), tmp_directory_path.joinpath(folder_name)
                )  # TODO ignore __pycache__

            module_name = "tmp_aws_s3_diff"
            spec = importlib.util.spec_from_file_location(
                module_name, tmp_directory_path.joinpath("aws_s3_diff/aws_s3_diff.py")
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            module_name = "tmp_local_results"
            spec_local_results = importlib.util.spec_from_file_location(
                module_name, tmp_directory_path.joinpath("aws_s3_diff/local_results.py")
            )
            module_local_results = importlib.util.module_from_spec(spec_local_results)
            sys.modules[module_name] = module_local_results

            spec.loader.exec_module(module)
            spec_local_results.loader.exec_module(module_local_results)
            with self._local_s3_server:
                for account in S3UrisFileReader().get_accounts():  # TODO use tmp dir
                    self._local_s3_server.create_objects(account)
                    # TODO i think that the analyzed date time is not created in tmp path
                    module.Main().run()
                    analysis_paths = module_local_results._AnalysisPaths(
                        self._get_analysis_date_time_str_new(tmp_directory_path.joinpath("s3-results"))
                    )
                    self._assert_extracted_accounts_data_have_expected_values(analysis_paths)
                    self._assert_analysis_file_has_expected_values(analysis_paths)

    @patch(
        "aws_s3_diff.aws_s3_diff.S3UrisFileReader._file_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis/s3-uris-to-analyze.csv"),
    )
    # TODO replace with test_run_if_should_work_ok_new
    def test_run_if_should_work_ok(self, mock_file_path_what_to_analyze):
        with self._local_s3_server:
            for account in S3UrisFileReader().get_accounts():
                self._local_s3_server.create_objects(account)
                Main().run()
        analysis_paths = _AnalysisPaths(self._get_analysis_date_time_str())
        self._assert_extracted_accounts_data_have_expected_values(analysis_paths)
        self._assert_analysis_file_has_expected_values(analysis_paths)
        shutil.rmtree(analysis_paths.directory_analysis)

    # TODO replace with _get_analysis_date_time_str_new
    def _get_analysis_date_time_str(self) -> str:
        analysis_directory_names = [
            directory_path.name for directory_path in LocalPaths().all_results_directory.glob("20*")
        ]
        analysis_directory_names.sort()
        return analysis_directory_names[-1]

    # TODO rename drop new
    def _get_analysis_date_time_str_new(self, all_results_directory_path: Path) -> str:
        analysis_directory_names = [directory_path.name for directory_path in all_results_directory_path.glob("20*")]
        # TODO? assert only 1 date folder and drop lines below
        analysis_directory_names.sort()
        return analysis_directory_names[-1]

    def _get_df_from_csv_expected_result(self) -> Df:
        current_path = Path(__file__).parent.absolute()
        expected_result_file_path = current_path.joinpath("expected-results", "analysis.csv")
        return self._get_df_from_csv(expected_result_file_path)

    def _get_df_from_csv(self, path: Path) -> Df:
        return read_csv(path).astype(
            {
                "pro_size": "Int64",
                "release_size": "Int64",
                "dev_size": "Int64",
            }
        )

    def _assert_extracted_accounts_data_have_expected_values(self, analysis_paths: _AnalysisPaths):
        for account, file_name_expected_result in {
            "pro": "pro.csv",
            "release": "release.csv",
            "dev": "dev.csv",
        }.items():
            file_path_results = analysis_paths.directory_analysis.joinpath(f"{account}.csv")
            result_df = read_csv(file_path_results)
            expected_result_df = read_csv(f"tests/expected-results/{file_name_expected_result}")
            expected_result_df["date"] = result_df["date"]
            assert_frame_equal(expected_result_df, result_df)

    def _assert_analysis_file_has_expected_values(self, analysis_paths: _AnalysisPaths):
        result = self._get_df_from_csv(analysis_paths.file_analysis)
        expected_result = self._get_df_from_csv_expected_result()
        date_column_names = ["pro_date", "release_date", "dev_date"]
        assert_frame_equal(expected_result.drop(columns=date_column_names), result.drop(columns=date_column_names))


class TestMainWithoutLocalS3Server(unittest.TestCase):
    @patch("aws_s3_diff.aws_s3_diff.Main._run_without_catching_exceptions")
    def test_run_manages_analysis_config_error_and_generates_expected_error_messages(self, mock_run):
        mock_run.side_effect = AnalysisConfigError("foo")
        with self.assertLogs(level="ERROR") as cm:
            Main().run()
        self.assertEqual("foo", cm.records[0].message)

    @patch("aws_s3_diff.aws_s3_diff.LocalResults")
    @patch("aws_s3_diff.aws_s3_diff.AnalyzedAccounts")
    @patch("aws_s3_diff.aws_s3_diff.AccountCsvCreator.export_csv")
    @patch(
        "aws_s3_diff.aws_s3_diff.S3UrisFileReader._file_path_what_to_analyze",
        new_callable=PropertyMock,
        return_value=Path(__file__).parent.absolute().joinpath("fake-files/test-full-analysis/s3-uris-to-analyze.csv"),
    )
    def test_run_manages_aws_client_errors_and_generates_expected_error_messages(
        self, mock_file_path_what_to_analyze, mock_extract, mock_analyzed_accounts, mock_local_results
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
        ):
            with self.subTest(expected_error_message=expected_error_message, aws_error=aws_error):
                mock_extract.side_effect = aws_error
                self._mock_to_not_generate_analysis_date_time_file(mock_analyzed_accounts, mock_local_results)
                with self.assertLogs(level="ERROR") as cm:
                    Main().run()
                self.assertEqual(expected_error_message, cm.records[0].message)

    def _mock_to_not_generate_analysis_date_time_file(self, mock_analyzed_accounts, mock_local_results):
        mock_analyzed_accounts().get_account_to_analyze.return_value = S3UrisFileReader().get_first_account()
        mock_analyzed_accounts().have_all_accounts_been_analyzed.return_value = False
        mock_local_results().analysis_paths.directory_analysis.is_dir.return_value = True
        mock_local_results().get_file_path_results().is_file.return_value = False
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
