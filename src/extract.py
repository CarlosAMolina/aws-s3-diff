import csv
import logging
import os
import os.path
from collections import namedtuple
from pathlib import PurePath

import boto3

from config import export_config as config
from constants import MAIN_FOLDER_NAME_EXPORTS

S3Query = namedtuple("S3Query", "bucket prefix")
S3FileData = dict
S3Data = list[S3FileData]


logger = logging.getLogger(__name__)


def run():
    if os.path.isdir(MAIN_FOLDER_NAME_EXPORTS):
        raise FileExistsError(f"The folder '{MAIN_FOLDER_NAME_EXPORTS}' exists, drop it before continue")
    for bucket_name in config.keys():
        exported_files_directory_path = _get_path_for_bucket_exported_files(bucket_name)
        print("Creating folder for bucket results: ", exported_files_directory_path)
        os.makedirs(exported_files_directory_path)
    s3_queries = _get_s3_queries()
    for query_index, s3_query in enumerate(s3_queries, 1):
        print(f"Working with query {query_index}/{len(s3_queries)}: {s3_query}")
        s3_data = _get_s3_data(s3_query)
        exported_files_directory_path = _get_path_for_bucket_exported_files(s3_query.bucket)
        file_path = _get_results_exported_file_path(exported_files_directory_path, s3_query.prefix)
        _export_data_to_csv(s3_data, file_path)
        print("Extraction done")


def _get_path_for_bucket_exported_files(bucket_name: str) -> PurePath:
    return PurePath(MAIN_FOLDER_NAME_EXPORTS, bucket_name)


def _get_s3_queries() -> list[S3Query]:
    return [S3Query(bucket, path_name) for bucket, path_names in config.items() for path_name in path_names]


def _get_s3_data(s3_query: S3Query) -> S3Data:
    session = boto3.Session()
    s3_client = session.client("s3")
    query_prefix = s3_query.prefix if s3_query.prefix.endswith("/") else f"{s3_query.prefix}/"
    _raise_exception_if_subfolders_in_s3(s3_client, s3_query.bucket, query_prefix)
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html
    operation_parameters = {"Bucket": s3_query.bucket, "Prefix": query_prefix}
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(**operation_parameters)
    result = []
    for page in page_iterator:
        page_files = [
            {
                "name": _get_file_name_from_response_key(content),
                "date": content["LastModified"],
                "size": content["Size"],
            }
            for content in page["Contents"]
        ]
        result += page_files
    return result


def _raise_exception_if_subfolders_in_s3(s3_client, bucket: str, query_prefix: str):
    # https://stackoverflow.com/questions/71577584/python-boto3-s3-list-only-current-directory-file-ignoring-subdirectory-files
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=query_prefix, Delimiter="/")
    if len(response.get("CommonPrefixes", [])) == 0:
        return
    folder_path_names = [common_prefix["Prefix"] for common_prefix in response["CommonPrefixes"]]
    error_text = (
        f"Subfolders detected in bucket {bucket}. This script cannot manage subfolders"
        f". Subfolders ({len(folder_path_names)}): {', '.join(folder_path_names)}"
    )
    raise ValueError(error_text)


def _get_file_name_from_response_key(content: dict) -> str:
    return content["Key"].split("/")[-1]


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
