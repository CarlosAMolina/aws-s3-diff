from pathlib import Path

from config import get_s3_uris_file_reader

FilePathNamesToCompare = tuple[str, str, str]


def run():
    _IteractiveMenu().run()
    # TODO S3DataComparator().run(config)


class _IteractiveMenu:
    def run(self):
        print("Welcome to the AWS S3 Diff tool!")
        self._show_aws_accounts_to_analyze()
        print("Checking if any AWS account has been analyzed")
        # if _LocalResults().has_any_account_been_analyzed():

    def _show_aws_accounts_to_analyze(self):
        print("AWS accounts configured to be analyzed:")
        aws_accounts = get_s3_uris_file_reader().get_aws_accounts()
        aws_accounts_list = [f"- {aws_account}" for aws_account in aws_accounts]
        print("\n".join(aws_accounts_list))


class _LocalResults:
    # TODO remove this file when the 3º account has been analyzed.
    _FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME = "/tmp/aws_s3_diff_analysis_date_time.txt"

    def has_any_account_been_analyzed(self) -> bool:
        return Path(self._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).is_file()

    @property
    def analysis_date_time_str(self) -> str:
        with open(self._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME) as file:
            return file.read()


if __name__ == "__main__":
    run()
