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
    if os.path.isdir(MAIN_FOLDER_NAME_EXPORTS):
        raise FileExistsError(f"The folder '{MAIN_FOLDER_NAME_EXPORTS}' exists, drop it before continue")
    path_config_files = Path(__file__).parent.absolute()
    path_with_folder_exported_s3_data = path_config_files
    config = Config(path_config_files, path_with_folder_exported_s3_data)
    for bucket_name in config.get_dict_s3_uris_to_analyze():
        exported_files_directory_path = _get_path_for_bucket_exported_files(bucket_name)
        print("Creating folder for bucket results: ", exported_files_directory_path)
        os.makedirs(exported_files_directory_path)
    s3_queries = _get_s3_queries(config)
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = S3Client().get_s3_data(s3_query)
        exported_files_directory_path = _get_path_for_bucket_exported_files(s3_query.bucket)
        file_path = _get_results_exported_file_path(exported_files_directory_path, s3_query.prefix)
        _export_data_to_csv(s3_data, file_path)
        print("Extraction done")


def _get_path_for_bucket_exported_files(bucket_name: str) -> PurePath:
    return PurePath(MAIN_FOLDER_NAME_EXPORTS, bucket_name)


def _get_s3_queries(config: Config) -> list[S3Query]:
    return [S3Query(bucket, path_name) for bucket, path_names in config.items() for path_name in path_names]


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
