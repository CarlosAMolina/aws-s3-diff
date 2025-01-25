import unittest
from unittest.mock import patch

from src.s3_client import FolderInS3UriError
from src.s3_client import S3Client
from types_custom import S3Query


class TestS3Client(unittest.TestCase):
    @patch("src.s3_client.boto3")
    def test_get_s3_data_raises_folder_error(self, mock_boto3):
        mock_boto3.Session().client().list_objects_v2.return_value = {
            "ResponseMetadata": {
                "RequestId": "foo",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {"x-amzn-requestid": "bar"},
                "RetryAttempts": 0,
            },
            "IsTruncated": False,
            "Name": "bucket-1",
            "Prefix": "tmp/",
            "Delimiter": "/",
            "MaxKeys": 1000,
            "CommonPrefixes": [{"Prefix": "tmp/folder/"}],
            "EncodingType": "url",
            "KeyCount": 1,
        }
        with self.assertRaises(FolderInS3UriError) as exception:
            for _ in S3Client().get_s3_data(S3Query("bucket-1", "tmp")):
                pass
        expected_error_message = (
            "Subfolders detected in bucket 'bucket-1'. The current version of the program cannot manage subfolders"
            ". Subfolders (1): tmp/folder/"
        )
        self.assertEqual(expected_error_message, str(exception.exception))
