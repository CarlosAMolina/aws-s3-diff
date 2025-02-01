from abc import ABC
from abc import abstractmethod

from botocore.exceptions import ClientError

from analysis import AnalysisS3DataFactory
from config_files import AnalysisConfigChecker
from config_files import AnalysisConfigReader
from config_files import S3UrisFileChecker
from config_files import S3UrisFileReader
from exceptions import AnalysisConfigError
from exceptions import FolderInS3UriError
from local_results import LocalResults
from logger import get_logger
from s3_data import AccountS3DataFactory
from s3_data import AllAccountsS3DataFactory


def run():
    _Main().run()


class _Main:
    def __init__(self):
        self._logger = get_logger()
        self._process_factory = _ProcessFactory()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        try:
            self._run_without_catching_exceptions()
        except (AnalysisConfigError, FolderInS3UriError) as exception:
            self._logger.error(exception)
            return
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        except ClientError as exception:
            error_code = exception.response["Error"]["Code"]
            if error_code == "NoSuchBucket":
                bucket_name = exception.response["Error"]["BucketName"]
                self._logger.error(
                    f"The bucket '{bucket_name}' does not exist. Specify a correct bucket and run the program again"
                )
                return
            if error_code in ("AccessDenied", "InvalidAccessKeyId"):
                self._logger.error("Incorrect AWS credentials. Authenticate and run the program again")
                return
            raise Exception from exception

    def _run_without_catching_exceptions(self):
        self._logger.info("Welcome to the AWS S3 Diff tool!")
        self._logger.debug("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_accounts_to_analyze()
        self._process_factory.get_process().run()

    def _show_accounts_to_analyze(self):
        accounts = self._s3_uris_file_reader.get_accounts()
        accounts_list = [f"\n{index}. {account}" for index, account in enumerate(accounts, 1)]
        self._logger.info(f"AWS accounts configured to be analyzed:{''.join(accounts_list)}")


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _ProcessFactory:
    def __init__(self):
        self._analyzed_accounts = _AnalyzedAccounts()
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _Process:
        """
        Some conditions avoid to generate a file if it exists.
        For example: the user drops the analysis file in order to run the program and generate the analysis again.
        """
        if self._local_results.analysis_paths.file_s3_data_all_accounts.is_file():
            return _AnalysisProcess()
        if self._analyzed_accounts.have_all_accounts_been_analyzed():
            return _NoCombinedS3DataProcess()
        return _AccountProcessFactory().get_process()


class _AnalyzedAccounts:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_account_to_analyze(self) -> str:
        accounts_to_analyze = self._s3_uris_file_reader.get_accounts()
        last_account_analyzed = self._get_last_account_analyzed()
        if last_account_analyzed is None:
            return self._s3_uris_file_reader.get_first_account()
        if last_account_analyzed == self._s3_uris_file_reader.get_last_account():
            # Unexpected situation. This method cannot be called if all accounts have been analyzed.
            raise RuntimeError("All AWS accounts have been analyzed")
        return accounts_to_analyze[accounts_to_analyze.index(last_account_analyzed) + 1]

    def have_all_accounts_been_analyzed(self) -> bool:
        return self._get_last_account_analyzed() == self._s3_uris_file_reader.get_last_account()

    def _get_last_account_analyzed(self) -> str | None:
        result = None
        for account in self._s3_uris_file_reader.get_accounts():
            if not self._local_results.has_this_account_been_analyzed(account):
                return result
            result = account
        return result


class _AccountProcess(_Process):
    def __init__(self, account: str):
        self._account = account
        self._account_s3_data_factory = AccountS3DataFactory(account)
        self._logger = get_logger()

    def run(self):
        self._logger.info(f"Analyzing the AWS account '{self._account}'")
        self._account_s3_data_factory.to_csv_extract_s3_data()


class _AccountProcessFactory:
    def __init__(self):
        self._analyzed_accounts = _AnalyzedAccounts()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _AccountProcess:
        account = self._analyzed_accounts.get_account_to_analyze()
        if account == self._s3_uris_file_reader.get_first_account():
            return _FirstAccountProcess(account)
        if account == self._s3_uris_file_reader.get_last_account():
            return _LastAccountProcess(account)
        return _IntermediateAccountProcess(account)


class _NoLastAccountProcess(_AccountProcess):
    def __init__(self, account: str):
        self._analyzed_accounts = _AnalyzedAccounts()
        super().__init__(account)

    def run(self):
        super().run()
        self._show_next_account_to_analyze()

    def _show_next_account_to_analyze(self):
        self._logger.info(
            f"The next account to be analyzed is '{self._analyzed_accounts.get_account_to_analyze()}'"
            ". Authenticate and run the program again"
        )


class _FirstAccountProcess(_NoLastAccountProcess):
    def __init__(self, account: str):
        super().__init__(account)
        self._local_results = LocalResults()

    def run(self):
        # The folder may exist but not the result file if an error occurred in the previous run,
        # e.g. errors interacting with S3.
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
        AllAccountsS3DataFactory().to_csv()
        _AnalysisProcess().run()


class _AnalysisProcess(_Process):
    def __init__(self):
        self._logger = get_logger()
        self._analysis_config_reader = AnalysisConfigReader()
        self._analysis_config_checker = AnalysisConfigChecker()
        self._analysis_s3_data_factory = AnalysisS3DataFactory()

    def run(self):
        if self._analysis_config_reader.must_run_analysis():
            self._analysis_config_checker.assert_file_is_correct()
            self._analysis_s3_data_factory.to_csv()
        else:
            self._logger.info("No analysis configured. Omitting")
        LocalResults().drop_file_with_analysis_date()


if __name__ == "__main__":
    run()
