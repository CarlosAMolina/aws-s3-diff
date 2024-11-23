import csv
import logging
import os
import os.path
from pathlib import Path

import boto3

from config import Config
from config import get_config
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
    file_path_for_results = config.get_local_path_file_query_results()
    AwsAccountExtractor(file_path_for_results, s3_queries).extract()


# TODO move it to _LocalResults
def _create_folders_for_buckets_results(config: Config):
    exported_files_directory_path = config.get_local_path_directory_bucket_results()
    print("Creating folder for bucket results: ", exported_files_directory_path)
    # TODO do it better
    if not Path(exported_files_directory_path).exists():
        os.makedirs(exported_files_directory_path)


class AwsAccountExtractor:
    def __init__(self, file_path_results: Path, s3_queries: list[S3Query]):
        self._file_path_results = file_path_results
        self._s3_queries = s3_queries

    def extract(self):
        print(f"Extracting AWS Account information to {self._file_path_results}")
        for query_index, s3_query in enumerate(self._s3_queries, 1):
            print(f"Running query {query_index}/{len(self._s3_queries)}: {s3_query}")
            s3_data = _S3Client().get_s3_data(s3_query)
            self._export_data_to_csv(s3_data, s3_query)
        print("Extraction done")

    def _export_data_to_csv(self, s3_data: S3Data, s3_query: S3Query):
        file_exists = self._file_path_results.exists()
        with open(self._file_path_results, "a", newline="") as f:
            # avoid ^M: https://stackoverflow.com/a/17725590
            headers = {**s3_query._asdict(), **s3_data[0]}.keys()
            w = csv.DictWriter(f, headers, lineterminator="\n")
            if not file_exists:
                w.writeheader()
            for file_data in s3_data:
                data = {**s3_query._asdict(), **file_data}
                w.writerow(data)


class _S3Client:
    def __init__(self):
        session = boto3.Session()
        self._s3_client = session.client("s3")

    def get_s3_data(self, s3_query: S3Query) -> S3Data:
        query_prefix = s3_query.prefix if s3_query.prefix.endswith("/") else f"{s3_query.prefix}/"
        self._raise_exception_if_subfolders_in_s3(s3_query.bucket, query_prefix)
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
        operation_parameters = {"Bucket": s3_query.bucket, "Prefix": query_prefix}
        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(**operation_parameters)
        result = []
        for page in page_iterator:
            if page["KeyCount"] == 0:
                if len(result) > 0:
                    raise ValueError("Not managed situation. Fix it to avoid lost data when returning empty result")
                # TODO try return empty list[dict] and check if all works ok,
                # TODO this is better to avoid two dicts to update when the headers change.
                return [
                    {
                        "name": None,
                        "date": None,
                        "size": None,
                    }
                ]
            page_files = [
                {
                    "name": self._get_file_name_from_response_key(content),
                    "date": content["LastModified"],
                    "size": content["Size"],
                }
                for content in page["Contents"]
            ]
            result += page_files
        return result

    def _raise_exception_if_subfolders_in_s3(self, bucket: str, query_prefix: str):
        # https://stackoverflow.com/questions/71577584/python-boto3-s3-list-only-current-directory-file-ignoring-subdirectory-files
        response = self._s3_client.list_objects_v2(Bucket=bucket, Prefix=query_prefix, Delimiter="/")
        if len(response.get("CommonPrefixes", [])) == 0:
            return
        folder_path_names = [common_prefix["Prefix"] for common_prefix in response["CommonPrefixes"]]
        error_text = (
            f"Subfolders detected in bucket {bucket}. This script cannot manage subfolders"
            f". Subfolders ({len(folder_path_names)}): {', '.join(folder_path_names)}"
        )
        raise ValueError(error_text)

    def _get_file_name_from_response_key(self, content: dict) -> str:
        return content["Key"].split("/")[-1]


if __name__ == "__main__":
    run()
