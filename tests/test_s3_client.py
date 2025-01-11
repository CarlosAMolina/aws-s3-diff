import unittest

from src.s3_client import FolderInS3UriError
from src.s3_client import S3Client
from tests.aws import S3Server
from types_custom import S3Query


class TestS3Client(unittest.TestCase):
    def setUp(self):
        self._local_s3_server = S3Server()
        self._local_s3_server.start()
        self._bucket_name = "bucket-1"
        self._local_s3_server.create_objects_from_memory(self._bucket_name)

    def tearDown(self):
        self._local_s3_server.stop()

    def test_get_s3_data_raises_folder_error(self):
        with self.assertRaises(FolderInS3UriError) as exception:
            S3Client().get_s3_data(S3Query(self._bucket_name, "tmp"))
        expected_error_message = (
            "Subfolders detected in bucket 'bucket-1'. The current version of the program cannot manage subfolders"
            ". Subfolders (1): tmp/folder/"
        )
        self.assertEqual(expected_error_message, str(exception.exception))
