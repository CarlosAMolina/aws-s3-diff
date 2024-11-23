from pathlib import Path

from src.local_results import LocalResults


def remove_file_with_analysis_date_if_exists():
    if Path(LocalResults._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).is_file():
        Path(LocalResults._FILE_PATH_NAME_ACCOUNTS_ANALYSIS_DATE_TIME).unlink()
