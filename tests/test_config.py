import unittest
from pathlib import Path

from src import config as m_config


class TestS3UrisFileReader(unittest.TestCase):
    def test_get_bucket_from_s3_uri(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config._S3UrisFileReader(Path("foo"))._get_bucket_from_s3_uri(uri)
        self.assertEqual("my-bucket", result)

    def test_get_path_from_s3_uri(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config._S3UrisFileReader(Path("foo"))._get_path_from_s3_uri(uri)
        self.assertEqual("my-folder/my-object.png", result)
