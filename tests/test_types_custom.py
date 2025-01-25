import unittest

from src.types_custom import S3Query


class TestS3Query(unittest.TestCase):
    def test_prefix_adds_slash_if_original_prefix_does_not_end_in_slash(self):
        self.assertEqual("bar/", S3Query("foo", "bar").prefix)

    def test_repr_if_original_prefix_does_not_end_in_slash(self):
        self.assertEqual("s3://foo/bar/", str(S3Query("foo", "bar")))

    def test_equals_if_different_prefix_slash_end(self):
        prefix = "bar/baz"
        self.assertEqual(S3Query("foo", prefix), S3Query("foo", f"{prefix}/"))
