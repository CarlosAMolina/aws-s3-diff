import unittest

from src import config as m_config


class TestS3UriParts(unittest.TestCase):
    def test_bucket(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config._S3UriParts(uri).bucket
        self.assertEqual("my-bucket", result)

    def test_key(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config._S3UriParts(uri).key
        self.assertEqual("my-folder/my-object.png", result)
