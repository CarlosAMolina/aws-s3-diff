import unittest

from pandas import MultiIndex
from pandas.testing import assert_index_equal

from src.combine import _S3UriDfModifier


class TestS3UriDfModifier(unittest.TestCase):
    def test_get_new_multi_index(self):
        multi_index_old = MultiIndex.from_tuples(
            [
                ("cars", "europe/spain", "cars-20241014.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20240914.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241015.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241019.csv"),
                ("pets_dev", "dogs/size/heavy", "dogs-20241021.csv"),
                ("pets_dev", "horses/europe", "horses-20210219.csv"),
                ("pets_dev", "non-existent-prefix", None),
            ],
            names=["bucket", "prefix", "name"],
        )
        result = _S3UriDfModifier("foo", "bar", "TODO")._get_new_multi_index(multi_index_old, "TODO")
        expected_result = MultiIndex.from_tuples(
            [
                ("cars", "europe/spain", "cars-20241014.csv"),
                ("pets", "dogs/big_size", "dogs-20240914.csv"),
                ("pets", "dogs/big_size", "dogs-20241015.csv"),
                ("pets", "dogs/big_size", "dogs-20241019.csv"),
                ("pets", "dogs/big_size", "dogs-20241021.csv"),
                ("pets", "horses/europe", "horses-20210219.csv"),
                ("pets", "non-existent-prefix", None),
            ],
            names=["bucket", "prefix", "name"],
        )
        assert_index_equal(expected_result, result)
