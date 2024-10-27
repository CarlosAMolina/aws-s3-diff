import csv
import logging
import os
import os.path
from pathlib import Path
from pathlib import PurePath

from config import Config
from constants import MAIN_FOLDER_NAME_EXPORTS
from s3 import S3Client
from types_custom import S3Data
from types_custom import S3Query

logger = logging.getLogger(__name__)


def run():
    config = _get_config()
    _run_using_config(config)


def _run_using_config(config: Config):
    _create_folders_for_buckets_results(config.get_bucket_names_to_analyze())
    s3_queries = config.get_s3_queries()
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = S3Client().get_s3_data(s3_query)
        file_path_for_results = _get_path_for_file_with_query_results(s3_query)
        _export_data_to_csv(s3_data, file_path_for_results)
        print("Extraction done")


def _get_config() -> Config:
    current_path = Path(__file__).parent.absolute()
    return Config(path_config_files=current_path, path_with_folder_exported_s3_data=current_path)


def _create_folders_for_buckets_results(bucket_names: list[str]):
    if os.path.isdir(MAIN_FOLDER_NAME_EXPORTS):
        raise FileExistsError(f"The folder '{MAIN_FOLDER_NAME_EXPORTS}' exists, drop it before continue")
    for bucket_name in bucket_names:
        exported_files_directory_path = _get_path_for_bucket_results(bucket_name)
        print("Creating folder for bucket results: ", exported_files_directory_path)
        os.makedirs(exported_files_directory_path)


def _get_path_for_file_with_query_results(s3_query: S3Query) -> PurePath:
    exported_files_directory_path = _get_path_for_bucket_results(s3_query.bucket)
    return _get_results_exported_file_path(exported_files_directory_path, s3_query.prefix)


def _get_path_for_bucket_results(bucket_name: str) -> PurePath:
    return PurePath(MAIN_FOLDER_NAME_EXPORTS, bucket_name)


def _get_results_exported_file_path(
    exported_files_directory_path: PurePath,
    s3_path_name: str,
) -> PurePath:
    s3_path_name_clean = s3_path_name[:-1] if s3_path_name.endswith("/") else s3_path_name
    exported_file_name = s3_path_name_clean.replace("/", "-")
    exported_file_name = f"{exported_file_name}.csv"
    return exported_files_directory_path.joinpath(exported_file_name)


def _export_data_to_csv(s3_data: S3Data, file_path: PurePath):
    print(f"Exporting data to {file_path}")
    with open(file_path, "w", newline="") as f:
        w = csv.DictWriter(f, s3_data[0].keys())
        w.writeheader()
        for file_data in s3_data:
            w.writerow(file_data)


if __name__ == "__main__":
    run()
