import unittest

from aws_s3_diff import s3_uri as m_s3_uri


class TestS3UriPart(unittest.TestCase):
    def test_bucket(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_s3_uri.S3UriPart(uri).bucket
        self.assertEqual("my-bucket", result)

    def test_key(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_s3_uri.S3UriPart(uri).key
        self.assertEqual("my-folder/my-object.png", result)
