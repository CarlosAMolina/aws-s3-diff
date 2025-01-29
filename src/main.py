from abc import ABC
from abc import abstractmethod

from botocore.exceptions import ClientError

from analysis import AnalysisGenerator
from config_files import S3UrisFileChecker
from config_files import S3UrisFileReader
from exceptions import AnalysisConfigError
from exceptions import FolderInS3UriError
from local_results import LocalResults
from logger import get_logger
from s3_data import AwsAccountExtractor
from s3_data import NewAllAccountsS3DataDf


def run():
    logger = get_logger()
    try:
        _InteractiveMenu().run()
    except (AnalysisConfigError, FolderInS3UriError) as exception:
        logger.error(exception)
        return
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
    except ClientError as exception:
        error_code = exception.response["Error"]["Code"]
        if error_code == "NoSuchBucket":
            bucket_name = exception.response["Error"]["BucketName"]
            logger.error(
                f"The bucket '{bucket_name}' does not exist. Specify a correct bucket and run the program again"
            )
            return
        if error_code in ("AccessDenied", "InvalidAccessKeyId"):
            logger.error("Incorrect AWS credentials. Authenticate and run the program again")
            return
        raise Exception from exception


class _InteractiveMenu:
    def __init__(self):
        self._logger = get_logger()
        self._process_factory = _ProcessFactory()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        self._logger.info("Welcome to the AWS S3 Diff tool!")
        self._logger.debug("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_aws_accounts_to_analyze()
        self._process_factory.get_process().run()

    def _show_aws_accounts_to_analyze(self):
        aws_accounts = self._s3_uris_file_reader.get_aws_accounts()
        aws_accounts_list = [f"\n{index}. {aws_account}" for index, aws_account in enumerate(aws_accounts, 1)]
        self._logger.info(f"AWS accounts configured to be analyzed:{''.join(aws_accounts_list)}")


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _ProcessFactory:
    def __init__(self):
        self._analyzed_aws_accounts = _AnalyzedAwsAccounts()
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _Process:
        """
        Some conditions avoid to generate a file if it exists.
        For example: the user drops the analysis file in order to run the program and generate the analysis again.
        """
        if self._local_results.analysis_paths.file_s3_data_all_accounts.is_file():
            return _AnalysisProcess()
        if self._analyzed_aws_accounts.have_all_aws_accounts_been_analyzed():
            return _NoCombinedS3DataProcess()
        return _AwsAccountProcessFactory().get_process()


class _AnalyzedAwsAccounts:
    def __init__(self):
        self._local_results = LocalResults()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_aws_account_to_analyze(self) -> str:
        aws_accounts_to_analyze = self._s3_uris_file_reader.get_aws_accounts()
        last_aws_account_analyzed = self._get_last_aws_account_analyzed()
        if last_aws_account_analyzed is None:
            return self._s3_uris_file_reader.get_first_aws_account()
        if last_aws_account_analyzed == self._s3_uris_file_reader.get_last_aws_account():
            # Unexpected situation. This method cannot be called if all accounts have been analyzed.
            raise RuntimeError("All AWS accounts have been analyzed")
        return aws_accounts_to_analyze[aws_accounts_to_analyze.index(last_aws_account_analyzed) + 1]

    def have_all_aws_accounts_been_analyzed(self) -> bool:
        return self._get_last_aws_account_analyzed() == self._s3_uris_file_reader.get_last_aws_account()

    def _get_last_aws_account_analyzed(self) -> str | None:
        result = None
        for aws_account in self._s3_uris_file_reader.get_aws_accounts():
            if not self._local_results.has_this_aws_account_been_analyzed(aws_account):
                return result
            result = aws_account
        return result


class _AwsAccountProcess(_Process):
    def __init__(self, aws_account: str):
        self._aws_account = aws_account
        self._local_results = LocalResults()
        self._logger = get_logger()
        self._s3_uris_file_reader = S3UrisFileReader()

    def run(self):
        self._logger.info(f"Analyzing the AWS account '{self._aws_account}'")
        self._export_s3_data_of_account()

    def _export_s3_data_of_account(self):
        AwsAccountExtractor(
            self._local_results.get_file_path_aws_account_results(self._aws_account),
            self._s3_uris_file_reader.get_s3_queries_for_aws_account(self._aws_account),
        ).extract()


class _AwsAccountProcessFactory:
    def __init__(self):
        self._analyzed_aws_accounts = _AnalyzedAwsAccounts()
        self._s3_uris_file_reader = S3UrisFileReader()

    def get_process(self) -> _AwsAccountProcess:
        aws_account = self._analyzed_aws_accounts.get_aws_account_to_analyze()
        if aws_account == self._s3_uris_file_reader.get_first_aws_account():
            return _FirstAwsAccountProcess(aws_account)
        if aws_account == self._s3_uris_file_reader.get_last_aws_account():
            return _LastAwsAccountProcess(aws_account)
        return _IntermediateAccountProcess(aws_account)


class _NoLastAwsAccountProcess(_AwsAccountProcess):
    def __init__(self, aws_account: str):
        self._analyzed_aws_accounts = _AnalyzedAwsAccounts()
        super().__init__(aws_account)

    def run(self):
        super().run()
        self._show_next_account_to_analyze()

    def _show_next_account_to_analyze(self):
        self._logger.info(
            f"The next account to be analyzed is '{self._analyzed_aws_accounts.get_aws_account_to_analyze()}'"
            ". Authenticate and run the program again"
        )


class _FirstAwsAccountProcess(_NoLastAwsAccountProcess):
    def __init__(self, aws_account: str):
        super().__init__(aws_account)
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


class _IntermediateAccountProcess(_NoLastAwsAccountProcess):
    def run(self):
        super().run()


class _LastAwsAccountProcess(_AwsAccountProcess):
    def run(self):
        super().run()
        _NoCombinedS3DataProcess().run()


class _NoCombinedS3DataProcess(_Process):
    def run(self):
        NewAllAccountsS3DataDf().to_csv()
        _AnalysisProcess().run()


class _AnalysisProcess(_Process):
    def run(self):
        AnalysisGenerator().run()
        LocalResults().drop_file_with_analysis_date()


if __name__ == "__main__":
    run()
