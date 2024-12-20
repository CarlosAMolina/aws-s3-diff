import sys
from abc import ABC
from abc import abstractmethod

from analysis import S3DataAnalyzer
from local_results import LocalResults
from s3_data import export_s3_data_all_accounts_to_one_file
from s3_data import export_s3_data_of_account
from s3_uris_to_analyze import S3UrisFileAnalyzer
from s3_uris_to_analyze import S3UrisFileChecker


def run():
    _InteractiveMenu().run()


class _InteractiveMenu:
    def __init__(self):
        self._s3_uris_file_analyzer = S3UrisFileAnalyzer()
        self._process_factory = _ProcessFactory()

    def run(self):
        print("Welcome to the AWS S3 Diff tool!")
        print("Checking if the URIs to analyze configuration file is correct")
        S3UrisFileChecker().assert_file_is_correct()
        self._show_aws_accounts_to_analyze()
        self._process_factory.get_process().run()

    def _show_aws_accounts_to_analyze(self):
        print("AWS accounts configured to be analyzed:")
        aws_accounts = self._s3_uris_file_analyzer.get_aws_accounts()
        aws_accounts_list = [f"- {aws_account}" for aws_account in aws_accounts]
        print("\n".join(aws_accounts_list))


class _Process(ABC):
    @abstractmethod
    def run(self):
        pass


class _ProcessFactory:
    def __init__(self):
        self._analyzed_aws_accounts = _AnalyzedAwsAccounts()
        self._local_results = LocalResults()
        self._s3_uris_file_analyzer = S3UrisFileAnalyzer()

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
        self._s3_uris_file_analyzer = S3UrisFileAnalyzer()

    def get_aws_account_to_analyze(self) -> str:
        aws_accounts_to_analyze = self._s3_uris_file_analyzer.get_aws_accounts()
        last_aws_account_analyzed = self._get_last_aws_account_analyzed()
        if last_aws_account_analyzed is None:
            return self._s3_uris_file_analyzer.get_first_aws_account()
        if last_aws_account_analyzed == self._s3_uris_file_analyzer.get_last_aws_account():
            # Unexpected situation. This method cannot be called if all accounts have been analyzed.
            raise RuntimeError("All AWS accounts have been analyzed")
        return aws_accounts_to_analyze[aws_accounts_to_analyze.index(last_aws_account_analyzed) + 1]

    def have_all_aws_accounts_been_analyzed(self) -> bool:
        return self._get_last_aws_account_analyzed() == self._s3_uris_file_analyzer.get_last_aws_account()

    def _get_last_aws_account_analyzed(self) -> str | None:
        result = None
        for aws_account in self._s3_uris_file_analyzer.get_aws_accounts():
            if not self._local_results.has_this_aws_account_been_analyzed(aws_account):
                return result
            result = aws_account
        return result


class _AwsAccountProcess(_Process):
    def __init__(self, aws_account: str):
        self._aws_account = aws_account

    def run(self):
        print(f"The following AWS account will be analyzed: {self._aws_account}")
        self._exit_program_if_no_aws_credentials_in_terminal()
        export_s3_data_of_account(self._aws_account)

    def _exit_program_if_no_aws_credentials_in_terminal(self):
        # TODO try avoid user iteraction, for example, detect with python that no credentials have been set.
        print("Have you generated in you terminal the AWS credentials to authenticate in that AWS account?")
        while True:
            user_input = input("Y/n: ").lower()
            if user_input == "n":
                print("Generate the credentials to work with that AWS account and run the program again")
                sys.exit()
            if user_input == "y" or len(user_input) == 0:
                return


class _AwsAccountProcessFactory:
    def __init__(self):
        self._analyzed_aws_accounts = _AnalyzedAwsAccounts()
        self._s3_uris_file_analyzer = S3UrisFileAnalyzer()

    def get_process(self) -> _AwsAccountProcess:
        aws_account = self._analyzed_aws_accounts.get_aws_account_to_analyze()
        if aws_account == self._s3_uris_file_analyzer.get_first_aws_account():
            return _FirstAwsAccountProcess(aws_account)
        if aws_account == self._s3_uris_file_analyzer.get_last_aws_account():
            return _LastAwsAccountProcess(aws_account)
        return _IntermediateAccountProcess(aws_account)


class _FirstAwsAccountProcess(_AwsAccountProcess):
    def __init__(self, aws_account: str):
        super().__init__(aws_account)
        self._local_results = LocalResults()

    def run(self):
        # The folder may exist but not the result file if an error occurred in the previous run,
        # e.g. errors interacting with S3.
        if not self._local_results.analysis_paths.directory_analysis.is_dir():
            self._local_results.create_analysis_results_folder()
        super().run()


class _IntermediateAccountProcess(_AwsAccountProcess):
    def run(self):
        super().run()


class _LastAwsAccountProcess(_AwsAccountProcess):
    def run(self):
        super().run()
        _NoCombinedS3DataProcess().run()


class _AnalysisProcess(_Process):
    def run(self):
        S3DataAnalyzer().run()
        LocalResults().remove_file_with_analysis_date()


class _NoCombinedS3DataProcess(_Process):
    def run(self):
        export_s3_data_all_accounts_to_one_file()
        _AnalysisProcess().run()


if __name__ == "__main__":
    run()
