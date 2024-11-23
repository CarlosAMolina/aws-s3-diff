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


# TODO deprecate
def run():
    config = get_config()
    _run_using_config(config)


# TODO deprecate
def _run_using_config(config: Config):
    # TODO use _LocalResults
    _create_folders_for_buckets_results(config)
    s3_queries = config.get_s3_queries()
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = S3Client().get_s3_data(s3_query)
        file_path_for_results = config.get_local_path_file_query_results()
        _export_data_to_csv(s3_data, s3_query, file_path_for_results)
        print("Extraction done")


# TODO move it to _LocalResults
def _create_folders_for_buckets_results(config: Config):
    exported_files_directory_path = config.get_local_path_directory_bucket_results()
    print("Creating folder for bucket results: ", exported_files_directory_path)
    # TODO do it better
    if not Path(exported_files_directory_path).exists():
        os.makedirs(exported_files_directory_path)


class AwsAccountExtractor:
    def __init__(self, file_path_results: Path, s3_queries: list[S3Query]) -> None:
        self._file_path_results = file_path_results
        self._s3_queries = s3_queries

    def extract(self):
        print(f"Extracting AWS Account information to {self._file_path_results}")
        for query_index, s3_query in enumerate(self._s3_queries, 1):
            print(f"Running query {query_index}/{len(self._s3_queries)}: {s3_query}")
            s3_data = S3Client().get_s3_data(s3_query)
            _export_data_to_csv(s3_data, s3_query, self._file_path_results)
        print("Extraction done")


# TODO move to AwsAccountExtractor
def _export_data_to_csv(s3_data: S3Data, s3_query: S3Query, file_path: Path):
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
