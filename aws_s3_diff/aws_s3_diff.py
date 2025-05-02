from abc import ABC
from abc import abstractmethod

from botocore.exceptions import ClientError
from botocore.exceptions import EndpointConnectionError

from aws_s3_diff.accounts import AnalyzedAccounts
from aws_s3_diff.config_files import AnalysisConfigChecker
from aws_s3_diff.config_files import AnalysisConfigReader
from aws_s3_diff.config_files import S3UrisFileChecker
from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.exceptions import AnalysisConfigError
from aws_s3_diff.exceptions import FolderInS3UriError
from aws_s3_diff.local_results import ACCOUNTS_FILE_NAME
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
from aws_s3_diff.s3_data.all_accounts import AccountsDf
from aws_s3_diff.s3_data.analysis import AnalysisCsvCreator
from aws_s3_diff.s3_data.one_account import AccountCsvCreator

_logger = get_logger()


class Main:
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._process_creator = _ProcessSimpleFactory()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        try:
            self._run_without_catching_exceptions()
        except (AnalysisConfigError, FolderInS3UriError) as exception:
            _logger.error(exception)
            return
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        except ClientError as exception:
            error_code = exception.response["Error"]["Code"]
            if error_code == "NoSuchBucket":
                bucket_name = exception.response["Error"]["BucketName"]
                _logger.error(
                    f"The bucket '{bucket_name}' does not exist. Specify a correct bucket and run the program again"
                )
                return
            if error_code in ("AccessDenied", "InvalidAccessKeyId"):
                _logger.error("Incorrect AWS credentials. Authenticate and run the program again")
                return
            raise Exception from exception
        # TODO add test
        except EndpointConnectionError as exception:
            _logger.error(exception)

    def _run_without_catching_exceptions(self):
        _logger.info("Welcome to the AWS S3 Diff tool!")
        _logger.debug("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_accounts_to_analyze()
        while True:
            process = self._process_creator.get_process()
            process.run()
            if isinstance(process, _AnalysisProcess):
                break
            if isinstance(process, (_FirstAccountProcess, _IntermediateAccountProcess)):
                _logger.info(
                    f"The next account to be analyzed is '{self._analyzed_accounts.get_account_to_analyze()}'"
                    ". Authenticate and run the program again"
                )
                break

    def _show_accounts_to_analyze(self):
        accounts = self._s3_uris_file_reader.get_accounts()
        accounts_list = [f"\n{index}. {account}" for index, account in enumerate(accounts, 1)]
        _logger.info(f"AWS accounts configured to be analyzed:{''.join(accounts_list)}")


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _ProcessSimpleFactory:
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _Process:
        """
        Some conditions avoid to generate a file if it exists.
        For example: the user drops the analysis file in order to run the program and generate the analysis again.
        """
        # TODO not access attribute of attribute
        if self._local_results.get_file_path_results(ACCOUNTS_FILE_NAME).is_file():
            return _AnalysisProcess()
        if self._analyzed_accounts.have_all_accounts_been_analyzed():
            return _CombineS3DataProcess()
        account = self._analyzed_accounts.get_account_to_analyze()
        if account == self._s3_uris_file_reader.get_first_account():
            return _FirstAccountProcess()
        if account == self._s3_uris_file_reader.get_last_account():
            return _AccountProcess()
        return _IntermediateAccountProcess()


class _AccountProcess(_Process):
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._account_csv_creator = AccountCsvCreator()

    def run(self):
        _logger.info(f"Analyzing the AWS account '{self._analyzed_accounts.get_account_to_analyze()}'")
        self._account_csv_creator.export_csv()


class _FirstAccountProcess(_AccountProcess):
    def __init__(self):
        self._local_results = LocalResults()
        super().__init__()

    def run(self):
        self._local_results.create_directory_analysis()
        try:
            super().run()
        except Exception as exception:
            self._local_results.drop_file_with_analysis_date()
            self._local_results.drop_directory_analysis()
            raise exception


class _IntermediateAccountProcess(_AccountProcess):
    pass


class _CombineS3DataProcess(_Process):
    def run(self):
        AccountsDf().to_csv()


class _AnalysisProcess(_Process):
    def __init__(self):
        self._analysis_config_reader = AnalysisConfigReader()
        self._analysis_config_checker = AnalysisConfigChecker()
        self._analysis_csv_creator = AnalysisCsvCreator()

    def run(self):
        if self._analysis_config_reader.must_run_analysis():
            self._analysis_config_checker.assert_file_is_correct()
            self._analysis_csv_creator.export_csv()
        else:
            _logger.info("No analysis configured. Omitting")
        LocalResults().drop_file_with_analysis_date()
