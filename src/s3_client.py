import os

import boto3

from exceptions import FolderInS3UriError
from types_custom import S3Data
from types_custom import S3Query


class S3Client:
    S3_DATA_KEYS = ("name", "date", "size")

    def __init__(self):
        session = boto3.Session()
        # TODO? drop not used AWS_ENDPOINT
        self._s3_client = session.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT"))

    def get_s3_data(self, s3_query: S3Query) -> S3Data:
        # TODO move the logic to add the `/` to the S3Query object
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
                return [{key: None for key in self.S3_DATA_KEYS}]
            page_files = [
                {
                    self.S3_DATA_KEYS[0]: self._get_file_name_from_response_key(content),
                    self.S3_DATA_KEYS[1]: content["LastModified"],
                    self.S3_DATA_KEYS[2]: content["Size"],
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
            f"Subfolders detected in bucket '{bucket}'. The current version of the program cannot manage subfolders"
            f". Subfolders ({len(folder_path_names)}): {', '.join(folder_path_names)}"
        )
        raise FolderInS3UriError(error_text)

    def _get_file_name_from_response_key(self, content: dict) -> str:
        return content["Key"].split("/")[-1]
