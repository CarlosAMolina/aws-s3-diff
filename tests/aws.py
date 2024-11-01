import os
from pathlib import Path

import boto3


# TODO use this in test_s3.py
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
    def __init__(self, endpoint_url: str | None = None):
        self._s3_resource = boto3.resource("s3", endpoint_url=endpoint_url)
        self._s3_client = boto3.client("s3", endpoint_url=endpoint_url)
        current_path = Path(__file__).parent.absolute()
        self._local_s3_objects_path = current_path.joinpath("s3-files")

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
        bucket_name = "pets"
        s3_file_path_name = "dogs/big_size/dogs-20241019.csv"
        local_file_path = self._local_s3_objects_path.joinpath(bucket_name, s3_file_path_name)
        self._upload_file(bucket_name, local_file_path, s3_file_path_name)

    def _upload_file(self, bucket_name: str, local_file_path: Path, s3_file_path_name: str):
        # TODO try to use
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
        # to avoid the `with open`.
        with open(local_file_path, "rb") as data:
            self._s3_client.upload_fileobj(data, bucket_name, s3_file_path_name)

    def _get_bucket_names_to_create(self) -> list[str]:
        """https://stackoverflow.com/questions/45870945/use-os-listdir-to-show-directories-only"""
        return [
            file_name
            for file_name in os.listdir(self._local_s3_objects_path)
            if os.path.isdir(self._local_s3_objects_path.joinpath(file_name))
        ]

    # TODO def _upload_directory(self, path, bucketname):
    # TODO     """https://stackoverflow.com/questions/25380774/upload-a-directory-to-s3-with-boto"""
    # TODO     for root,dirs,files in os.walk(path):
    # TODO         for file in files:
    # TODO             s3C.upload_file(os.path.join(root,file),bucketname,file)
