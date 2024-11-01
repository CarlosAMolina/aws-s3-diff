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

    def create_objects(self):
        bucket_name = "pets"
        self._create_bucket(bucket_name)
        current_path = Path(__file__).parent.absolute()
        local_s3_files_path = current_path.joinpath("s3-files")
        s3_file_path_name = "dogs/big_size/dogs-20241019.csv"
        local_file_path = local_s3_files_path.joinpath(bucket_name, s3_file_path_name)
        self._upload_file(bucket_name, local_file_path, s3_file_path_name)

    def _create_bucket(self, bucket_name: str):
        bucket = self._s3_resource.Bucket(bucket_name)
        bucket.create()

    def _upload_file(self, bucket_name: str, local_file_path: Path, s3_file_path_name: str):
        with open(local_file_path, "rb") as data:
            self._s3_client.upload_fileobj(data, bucket_name, s3_file_path_name)
