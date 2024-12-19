import os
from pathlib import Path

import boto3
from moto import mock_aws


# TODO use with open for mock_aws
class S3Server:
    def __init__(self):
        """http://docs.getmoto.org/en/latest/docs/getting_started.html"""
        set_aws_credentials()
        self._mock_aws = mock_aws()

    def start(self):
        self._mock_aws.start()

    def create_objects(self, aws_account):
        S3(aws_account).create_objects()

    def stop(self):
        self._mock_aws.stop()


def set_aws_credentials():
    """ "
    http://docs.getmoto.org/en/latest/docs/getting_started.html#how-do-i-avoid-tests-from-mutating-my-real-infrastructure
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


class S3:
    def __init__(self, aws_account: str, endpoint_url: str | None = None):
        self._s3_resource = boto3.resource("s3", endpoint_url=endpoint_url)
        self._s3_client = boto3.client("s3", endpoint_url=endpoint_url)
        current_path = Path(__file__).parent.absolute()
        self._local_s3_objects_path = current_path.joinpath("fake-files/s3-files", aws_account)

    def create_objects(self):
        self._create_buckets()
        self._upload_files()

    def _create_buckets(self):
        for bucket_name in self._get_bucket_names_to_create():
            self._create_bucket(bucket_name)

    def _create_bucket(self, bucket_name: str):
        bucket = self._s3_resource.Bucket(bucket_name)
        bucket.create()

    def _upload_files(self):
        for bucket_name in self._get_bucket_names_to_create():
            bucket_name_local_path = self._local_s3_objects_path.joinpath(bucket_name)
            for local_file_path in self._get_file_paths_in_directory_path(bucket_name_local_path):
                s3_file_path_name = str(local_file_path.relative_to(bucket_name_local_path))
                self._upload_file(bucket_name, local_file_path, s3_file_path_name)

    def _upload_file(self, bucket_name: str, local_file_path: Path, s3_file_path_name: str):
        self._s3_client.upload_file(local_file_path, bucket_name, s3_file_path_name)

    def _get_bucket_names_to_create(self) -> list[str]:
        """https://stackoverflow.com/questions/45870945/use-os-listdir-to-show-directories-only"""
        return [
            file_name
            for file_name in os.listdir(self._local_s3_objects_path)
            if os.path.isdir(self._local_s3_objects_path.joinpath(file_name))
        ]

    def _get_file_paths_in_directory_path(self, directory_path: Path) -> list[Path]:
        """https://stackoverflow.com/questions/25380774/upload-a-directory-to-s3-with-boto"""
        return [Path(root, file_name) for root, _dirs, files in os.walk(directory_path) for file_name in files]
