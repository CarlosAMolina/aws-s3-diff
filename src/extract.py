import csv
import logging
import os
import os.path
from pathlib import Path

from config import Config
from config import get_config
from s3 import S3Client
from types_custom import S3Data

logger = logging.getLogger(__name__)


def run():
    config = get_config()
    _run_using_config(config)


def _run_using_config(config: Config):
    _create_folders_for_buckets_results(config)
    s3_queries = config.get_s3_queries()
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = S3Client().get_s3_data(s3_query)
        file_path_for_results = config.get_local_path_file_query_results(s3_query)
        _export_data_to_csv(s3_data, file_path_for_results)
        print("Extraction done")


def _create_folders_for_buckets_results(config: Config):
    for bucket_name in config.get_bucket_names_to_analyze():
        exported_files_directory_path = config.get_local_path_directory_bucket_results(bucket_name)
        print("Creating folder for bucket results: ", exported_files_directory_path)
        os.makedirs(exported_files_directory_path)


def _export_data_to_csv(s3_data: S3Data, file_path: Path):
    print(f"Exporting data to {file_path}")
    with open(file_path, "w", newline="") as f:
        # avoid ^M: https://stackoverflow.com/a/17725590
        w = csv.DictWriter(f, s3_data[0].keys(), lineterminator="\n")
        w.writeheader()
        for file_data in s3_data:
            w.writerow(file_data)


if __name__ == "__main__":
    run()
