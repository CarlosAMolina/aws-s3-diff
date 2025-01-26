import os
from collections.abc import Iterator
from pathlib import Path

import boto3

from exceptions import FolderInS3UriError
from types_custom import FileS3Data
from types_custom import S3Data
from types_custom import S3Query


class S3Client:
    def __init__(self):
        self._s3_client = boto3.Session().client("s3", endpoint_url=os.getenv("AWS_ENDPOINT"))

    def get_s3_data(self, s3_query: S3Query) -> Iterator[S3Data]:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html"""
        response = self._s3_client.list_objects_v2(**self._get_request_arguments(s3_query))
        while response["KeyCount"] != 0:
            self._raise_exception_if_folders_in_response(response, s3_query.bucket)
            yield self._get_s3_data_from_response(response)
            last_key = response["Contents"][-1]["Key"]
            response = self._s3_client.list_objects_v2(**self._get_request_arguments(s3_query, last_key))

    def _get_request_arguments(self, s3_query: S3Query, last_key: str | None = None) -> dict:
        max_keys = int(os.getenv("AWS_MAX_KEYS", 1000))
        result = {
            "Bucket": s3_query.bucket,
            "Prefix": s3_query.prefix,
            "MaxKeys": max_keys,
            "Delimiter": "/",  # Required for folders detection.
        }
        if last_key:
            result["StartAfter"] = last_key
        return result

    def _raise_exception_if_folders_in_response(self, response: dict, bucket: str):
        folder_path_names = self._get_folder_path_names_in_response_list_objects_v2(response)
        if len(folder_path_names) == 0:
            return
        folder_path_names = [common_prefix["Prefix"] for common_prefix in response["CommonPrefixes"]]
        error_text = (
            f"Subfolders detected in bucket '{bucket}'. The current version of the program cannot manage subfolders"
            f". Subfolders ({len(folder_path_names)}): {', '.join(folder_path_names)}"
        )
        raise FolderInS3UriError(error_text)

    def _get_folder_path_names_in_response_list_objects_v2(self, response: dict) -> list[str]:
        # Detect folders: https://stackoverflow.com/a/71579041
        if "CommonPrefixes" not in response:
            return []
        return [common_prefix["Prefix"] for common_prefix in response["CommonPrefixes"]]

    def _get_s3_data_from_response(self, response: dict) -> S3Data:
        return [self._get_file_s3_data_from_s3_response_content(content) for content in response["Contents"]]

    @staticmethod
    def _get_file_s3_data_from_s3_response_content(s3_response_content: dict) -> FileS3Data:
        return FileS3Data(
            Path(s3_response_content["Key"]).name,
            s3_response_content["LastModified"],
            s3_response_content["Size"],
            s3_response_content["ETag"].strip('"'),
        )
