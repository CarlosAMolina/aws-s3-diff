import os

from constants import MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS


def get_aws_accounts() -> list[str]:
    result = os.listdir(MAIN_FOLDER_NAME_EXPORTS_ALL_AWS_ACCOUNTS)
    result.sort()
    return result
