import csv
import logging
import os
import os.path
from pathlib import Path

from config import Config
from config import get_config
from s3 import S3Client
from types_custom import S3Data
from types_custom import S3Query

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
        _export_data_to_csv(s3_data, s3_query, file_path_for_results)
        print("Extraction done")


def _create_folders_for_buckets_results(config: Config):
    exported_files_directory_path = config.get_local_path_directory_bucket_results()
    print("Creating folder for bucket results: ", exported_files_directory_path)
    # TODO do it better
    if not Path(exported_files_directory_path).exists():
        os.makedirs(exported_files_directory_path)


def _export_data_to_csv(s3_data: S3Data, s3_query: S3Query, file_path: Path):
    print(f"Exporting data to {file_path}")
    file_exists = file_path.exists()
    with open(file_path, "a", newline="") as f:
        # avoid ^M: https://stackoverflow.com/a/17725590
        headers = {**s3_query._asdict(), **s3_data[0]}.keys()
        w = csv.DictWriter(f, headers, lineterminator="\n")
        if not file_exists:
            w.writeheader()
        for file_data in s3_data:
            data = {**s3_query._asdict(), **file_data}
            w.writerow(data)


if __name__ == "__main__":
    run()
