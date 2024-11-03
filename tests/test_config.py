import unittest
from pathlib import Path

from src import config as m_config


class TestConfig(unittest.TestCase):
    def test_get_bucket_and_path_from_s3_uri(self):
        uri = "s3://my-bucket/my-folder/my-object.png"
        result = m_config.Config(Path("foo"), Path("foo"))._get_bucket_and_path_from_s3_uri(uri)
        expected_result = ("my-bucket", "my-folder/my-object.png")
        self.assertEqual(expected_result, result)
