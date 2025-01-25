import os
from collections.abc import Iterator

import boto3

from exceptions import FolderInS3UriError
from types_custom import FileS3Data
from types_custom import S3Data
from types_custom import S3Query


class S3Client:
    def __init__(self):
        session = boto3.Session()
        self._s3_client = session.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT"))

    def get_s3_data(self, s3_query: S3Query) -> Iterator[S3Data]:
        """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html"""
        last_key = ""
        is_any_s3_file = False
        while True:
            response = self._s3_client.list_objects_v2(**self._get_request_arguments(last_key, s3_query))
            self._raise_exception_if_folders_in_response(response, s3_query.bucket)
            # When using `MaxKeys`, `IsTruncated` is True and we can't check if all objects were
            # retrieved with `response["IsTruncated"] is True`.
            # If S3 prefix only has a folder (no files), the response won't have the 'Contents' key,
            # it is important to check the key after review if there are folders.
            if response.get("Contents") is None:
                break
            is_any_s3_file = True
            last_key = response["Contents"][-1]["Key"]
            yield [_FileS3DataFromS3Content(content).file_s3_data for content in response["Contents"]]
        # It is important to return empty FileS3Data to save the query in the results file if no results.
        if is_any_s3_file is False:
            yield [FileS3Data()]

    def _get_request_arguments(self, last_key: str, s3_query: S3Query) -> dict:
        max_keys = int(os.getenv("AWS_MAX_KEYS", 1000))
        return {
            "Bucket": s3_query.bucket,
            "Prefix": s3_query.prefix,
            "MaxKeys": max_keys,
            "StartAfter": last_key,
            "Delimiter": "/",  # Required for folders detection.
        }

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


class _FileS3DataFromS3Content:
    def __init__(self, s3_response_content: dict):
        self._s3_response_content = s3_response_content

    @property
    def file_s3_data(self) -> FileS3Data:
        return FileS3Data(
            self._get_file_name_from_response_key(self._s3_response_content),
            self._s3_response_content["LastModified"],
            self._s3_response_content["Size"],
            self._s3_response_content["ETag"].strip('"'),
        )

    def _get_file_name_from_response_key(self, content: dict) -> str:
        # TODO use Path
        return content["Key"].split("/")[-1]
