from abc import ABC
from abc import abstractmethod

from botocore.exceptions import ClientError
from botocore.exceptions import EndpointConnectionError
from pandas import DataFrame as Df

from aws_s3_diff.accounts import AnalyzedAccounts
from aws_s3_diff.config_files import AnalysisConfigChecker
from aws_s3_diff.config_files import AnalysisConfigReader
from aws_s3_diff.config_files import S3UrisFileChecker
from aws_s3_diff.config_files import S3UrisFileReader
from aws_s3_diff.exceptions import AnalysisConfigError
from aws_s3_diff.exceptions import FolderInS3UriError
from aws_s3_diff.exceptions import S3UrisFileError
from aws_s3_diff.local_results import ACCOUNTS_FILE_NAME
from aws_s3_diff.local_results import LocalResults
from aws_s3_diff.logger import get_logger
from aws_s3_diff.s3_data.all_accounts import AccountsCsvCreator
from aws_s3_diff.s3_data.analysis import AnalysisCsvCreator
from aws_s3_diff.s3_data.one_account import AccountCsvCreator

_logger = get_logger()


class Main:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_checker = S3UrisFileChecker()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        _logger.info("Welcome to the AWS S3 Diff tool!")
        _logger.debug("Checking if the URIs to analyze configuration file is correct")
        try:
            self._s3_uris_file_checker.assert_file_is_correct()
        except S3UrisFileError as exception:
            _logger.error(exception)
            return
        self._show_accounts_to_analyze()
        if not self._local_results.exist_directory_analysis():
            self._local_results.create_directory_analysis()
        self._export_csvs()

    def _show_accounts_to_analyze(self):
        accounts = self._s3_uris_file_reader.get_accounts()
        accounts_list = [f"\n{index}. {account}" for index, account in enumerate(accounts, 1)]
        _logger.info(f"AWS accounts configured to be analyzed:{''.join(accounts_list)}")

    def _export_csvs(self):
        s3_diff_process = _S3DiffProcess()
        while s3_diff_process.must_run_next_state:
            try:
                df = s3_diff_process.get_df()
            except (AnalysisConfigError, EndpointConnectionError, FolderInS3UriError) as exception:
                _logger.error(exception)
                return
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
            except ClientError as exception:
                error_code = exception.response["Error"]["Code"]
                if error_code == "NoSuchBucket":
                    _logger.error(self._get_error_message_no_such_bucket(exception))
                    return
                if error_code in ("AccessDenied", "InvalidAccessKeyId"):
                    _logger.error("Incorrect AWS credentials. Authenticate and run the program again")
                    return
                raise Exception from exception
            s3_diff_process.export_csv(df)

    def _get_error_message_no_such_bucket(self, exception: ClientError) -> str:
        bucket_name = exception.response["Error"]["BucketName"]
        return f"The bucket '{bucket_name}' does not exist. Specify a correct bucket and run the program again"


class _State(ABC):
    @abstractmethod
    def get_df(self) -> Df:
        pass

    @abstractmethod
    def export_csv(self, df: Df):
        pass


class _S3DiffProcess:
    def __init__(self):
        self._account_state = _AccountState(self)
        self._analysis_state = _AnalysisState(self)
        self._combine_state = _CombineState(self)
        self._must_run_next_state = True
        # TODO I prefer not to it in __init__
        if LocalResults().get_file_path_results(ACCOUNTS_FILE_NAME).is_file():
            self.set_state_analysis()
        elif AnalyzedAccounts().have_all_accounts_been_analyzed():
            self.set_state_combine()
        else:
            self.set_state_account()

    def get_df(self) -> Df:
        return self._state.get_df()

    def export_csv(self, df: Df):
        self._state.export_csv(df)

    def set_state_account(self):
        self._state = self._account_state

    def set_state_analysis(self):
        self._state = self._analysis_state

    def set_state_combine(self):
        self._state = self._combine_state

    @property
    def must_run_next_state(self) -> bool:
        return self._must_run_next_state

    def set_must_not_run_next_state(self):
        self._must_run_next_state = False


class _AccountState(_State):
    def __init__(self, s3_diff_process: _S3DiffProcess):
        self._s3_diff_process = s3_diff_process
        self._analyzed_accounts = AnalyzedAccounts()
        self._csv_creator = AccountCsvCreator()

    def get_df(self) -> Df:
        _logger.info(f"Analyzing the AWS account '{self._analyzed_accounts.get_account_to_analyze()}'")
        return self._csv_creator.get_df()

    def export_csv(self, df: Df):
        self._csv_creator.export_csv(df)
        if self._analyzed_accounts.have_all_accounts_been_analyzed():
            self._s3_diff_process.set_state_combine()
        else:
            _logger.info(
                f"The next account to be analyzed is '{self._analyzed_accounts.get_account_to_analyze()}'"
                ". Authenticate and run the program again"
            )
            self._s3_diff_process.set_must_not_run_next_state()


class _CombineState(_State):
    def __init__(self, s3_diff_process: _S3DiffProcess):
        self._s3_diff_process = s3_diff_process
        self._csv_creator = AccountsCsvCreator()

    def get_df(self) -> Df:
        return self._csv_creator.get_df()

    def export_csv(self, df: Df):
        self._csv_creator.export_csv(df)
        self._s3_diff_process.set_state_analysis()


class _AnalysisState(_State):
    def __init__(self, s3_diff_process: _S3DiffProcess):
        self._s3_diff_process = s3_diff_process
        self._analysis_config_reader = AnalysisConfigReader()
        self._analysis_config_checker = AnalysisConfigChecker()
        self._csv_creator = AnalysisCsvCreator()
        self._local_results = LocalResults()

    def get_df(self) -> Df:
        if self._analysis_config_reader.must_run_analysis():
            self._analysis_config_checker.assert_file_is_correct()
            return self._csv_creator.get_df()
        _logger.info("No analysis configured. Omitting")
        return Df()

    def export_csv(self, df: Df):
        if df.empty:
            return
        self._csv_creator.export_csv(df)
        self._local_results.drop_file_with_analysis_date()
        self._s3_diff_process.set_must_not_run_next_state()
