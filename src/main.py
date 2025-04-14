from abc import ABC
from abc import abstractmethod

from botocore.exceptions import ClientError

from accounts import AnalyzedAccounts
from config_files import AnalysisConfigChecker
from config_files import AnalysisConfigReader
from config_files import S3UrisFileChecker
from config_files import S3UrisFileReader
from exceptions import AnalysisConfigError
from exceptions import FolderInS3UriError
from local_results import LocalResults
from logger import get_logger
from s3_data.all_accounts import AccountsDf
from s3_data.analysis import AnalysisCsvCreator
from s3_data.one_account import AccountCsvCreator

_logger = get_logger()


class _Main:
    def __init__(self):
        self._process_creator = _ProcessCreator()
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

    def _run_without_catching_exceptions(self):
        _logger.info("Welcome to the AWS S3 Diff tool!")
        _logger.debug("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_accounts_to_analyze()
        self._process_creator.get_process().run()

    def _show_accounts_to_analyze(self):
        accounts = self._s3_uris_file_reader.get_accounts()
        accounts_list = [f"\n{index}. {account}" for index, account in enumerate(accounts, 1)]
        _logger.info(f"AWS accounts configured to be analyzed:{''.join(accounts_list)}")


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _ProcessCreator:
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._local_results = LocalResults()

    def get_process(self) -> _Process:
        """
        Some conditions avoid to generate a file if it exists.
        For example: the user drops the analysis file in order to run the program and generate the analysis again.
        """
        # TODO not access attribute of attribute
        if self._local_results.analysis_paths.file_s3_data_all_accounts.is_file():
            return _AnalysisProcess()
        if self._analyzed_accounts.have_all_accounts_been_analyzed():
            return _NoCombinedS3DataProcess()
        return _AccountProcessCreator().get_process()


class _AccountProcess(_Process):
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._account_csv_creator = AccountCsvCreator()

    def run(self):
        _logger.info(f"Analyzing the AWS account '{self._analyzed_accounts.get_account_to_analyze()}'")
        self._account_csv_creator.export_csv()


class _AccountProcessCreator:
    def __init__(self):
        self._analyzed_accounts = AnalyzedAccounts()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _AccountProcess:
        account = self._analyzed_accounts.get_account_to_analyze()
        if account == self._s3_uris_file_reader.get_first_account():
            return _FirstAccountProcess()
        if account == self._s3_uris_file_reader.get_last_account():
            return _LastAccountProcess()
        return _IntermediateAccountProcess()


class _NoLastAccountProcess(_AccountProcess):
    def run(self):
        super().run()
        self._show_next_account_to_analyze()

    def _show_next_account_to_analyze(self):
        _logger.info(
            f"The next account to be analyzed is '{self._analyzed_accounts.get_account_to_analyze()}'"
            ". Authenticate and run the program again"
        )


class _FirstAccountProcess(_NoLastAccountProcess):
    def __init__(self):
        self._local_results = LocalResults()
        super().__init__()

    def run(self):
        # The folder may exist but not the result file if an error occurred in the previous run,
        # e.g. errors interacting with S3.
        # TODO not access attribute of attribute
        if not self._local_results.analysis_paths.directory_analysis.is_dir():
            self._local_results.create_directory_analysis()
        try:
            super().run()
        except Exception as exception:
            self._local_results.drop_file_with_analysis_date()
            self._local_results.drop_directory_analysis()
            raise exception


class _IntermediateAccountProcess(_NoLastAccountProcess):
    def run(self):
        super().run()


class _LastAccountProcess(_AccountProcess):
    def run(self):
        super().run()
        _NoCombinedS3DataProcess().run()


class _NoCombinedS3DataProcess(_Process):
    def run(self):
        AccountsDf().to_csv()
        _AnalysisProcess().run()


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


if __name__ == "__main__":
    _Main().run()
