import os

import boto3

from exceptions import FolderInS3UriError
from types_custom import FileS3Data
from types_custom import S3Data
from types_custom import S3Query


class S3Client:
    def __init__(self):
        session = boto3.Session()
        self._s3_client = session.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT"))

    def get_s3_data(self, s3_query: S3Query) -> S3Data:
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
        """
        # TODO move the logic to add the `/` to the S3Query object
        query_prefix = s3_query.prefix if s3_query.prefix.endswith("/") else f"{s3_query.prefix}/"
        last_key = ""
        result = []
        while True:
            response = self._s3_client.list_objects_v2(
                Bucket=s3_query.bucket,
                Prefix=query_prefix,
                MaxKeys=os.getenv("AWS_MAX_KEYS", 1_000),
                StartAfter=last_key,
                Delimiter="/",  # Required for folders detection.
            )
            """
            `response["IsTruncated"] is True` is not valid to know if all objects were
            retrieved because when using `MaxKeys`, `IsTruncated` is True.
            """
            if response.get("Contents") is None:
                break
            self._raise_exception_if_folders_in_response(response, s3_query.bucket)
            # TODO use yield
            result += [_FileS3DataFromS3Content(content).file_s3_data for content in response["Contents"]]
            last_key = response["Contents"][-1]["Key"]
        if len(result) == 0:
            result += [FileS3Data()]
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


class _FileS3DataFromS3Content:
    def __init__(self, s3_response_content: dict):
        self._s3_response_content = s3_response_content

    @property
    def file_s3_data(self) -> FileS3Data:
        return FileS3Data(
            self._get_file_name_from_response_key(self._s3_response_content),
            self._s3_response_content["LastModified"],
            self._s3_response_content["Size"],
        )

    def _get_file_name_from_response_key(self, content: dict) -> str:
        # TODO use Path
        return content["Key"].split("/")[-1]
