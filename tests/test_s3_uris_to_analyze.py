import unittest

from src import s3_uris_to_analyze as m_uris_to_analyze


class TestS3UriParts(unittest.TestCase):
    def test_bucket(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_uris_to_analyze._S3UriParts(uri).bucket
        self.assertEqual("my-bucket", result)

    def test_key(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_uris_to_analyze._S3UriParts(uri).key
        self.assertEqual("my-folder/my-object.png", result)
