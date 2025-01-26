import unittest

from pandas import DataFrame as Df
from pandas import MultiIndex
from pandas.testing import assert_frame_equal

from src import s3_data as m_s3_data

ExpectedResult = list[dict]


class TestS3UriDfModifier(unittest.TestCase):
    def test_get_df_set_s3_uris_in_origin_account(self):
        aws_account_origin = "aws_account_1_pro"
        aws_account_target = "aws_account_3_dev"
        df = Df(
            [
                ["cars_dev", "europe/spain/", "cars-20241014.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy/", "dogs-20240914.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy/", "dogs-20241015.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy/", "dogs-20241019.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy/", "dogs-20241021.csv"] + ["foo"] * 3,
                ["pets_dev", "horses/europe/", "horses-20210219.csv"] + ["foo"] * 3,
                ["pets_dev", "non-existent-prefix/", None] + ["foo"] * 3,
            ],
            columns=["bucket", "prefix", "name", "date", "size", "hash"],
        )
        df = self._get_df_as_multi_index(aws_account_target, df)
        s3_uris_map_df_prefix_does_not_end_with_slash = Df(
            {
                aws_account_origin: {
                    0: "s3://cars/europe/spain",
                    1: "s3://pets/dogs/big_size",
                    2: "s3://pets/horses/europe",
                    3: "s3://pets/non-existent-prefix",
                },
                aws_account_target: {
                    0: "s3://cars_dev/europe/spain",
                    1: "s3://pets_dev/dogs/size/heavy",
                    2: "s3://pets_dev/horses/europe",
                    3: "s3://pets_dev/non-existent-prefix",
                },
            }
        )
        s3_uris_map_df_prefix_ends_with_slash = s3_uris_map_df_prefix_does_not_end_with_slash.copy()
        for aws_account in (aws_account_origin, aws_account_target):
            s3_uris_map_df_prefix_ends_with_slash[aws_account] = (
                s3_uris_map_df_prefix_ends_with_slash[aws_account] + "/"
            )
        expected_result = Df(
            [
                ["cars", "europe/spain/", "cars-20241014.csv"] + ["foo"] * 3,
                ["pets", "dogs/big_size/", "dogs-20240914.csv"] + ["foo"] * 3,
                ["pets", "dogs/big_size/", "dogs-20241015.csv"] + ["foo"] * 3,
                ["pets", "dogs/big_size/", "dogs-20241019.csv"] + ["foo"] * 3,
                ["pets", "dogs/big_size/", "dogs-20241021.csv"] + ["foo"] * 3,
                ["pets", "horses/europe/", "horses-20210219.csv"] + ["foo"] * 3,
                ["pets", "non-existent-prefix/", None] + ["foo"] * 3,
            ],
            columns=["bucket", "prefix", "name", "date", "size", "hash"],
        )
        expected_result = self._get_df_as_multi_index(aws_account_target, expected_result)
        for test_data in (
            (
                df,
                s3_uris_map_df_prefix_ends_with_slash,
            ),
            (
                df,
                s3_uris_map_df_prefix_does_not_end_with_slash,
            ),
        ):
            df, s3_uris_map_df = test_data
            with self.subTest(df=df, s3_uris_map_df=s3_uris_map_df):
                assert_frame_equal(
                    expected_result,
                    m_s3_data._S3UriDfModifier(
                        aws_account_origin, aws_account_target, df
                    )._get_df_set_s3_uris_in_origin_account(s3_uris_map_df),
                )

    def _get_df_as_multi_index(self, aws_account_target: str, df: Df) -> Df:
        result = df.copy()
        result = result.set_index(["bucket", "prefix", "name"])
        result.columns = [[aws_account_target] * len(result.columns), result.columns]
        return result

    def test_get_df_modify_buckets_and_paths(self):
        old_multi_index = MultiIndex.from_tuples(
            [
                ("cars_dev", "europe/spain/", "cars-20241014.csv"),
                ("pets_dev", "dogs/size/heavy/", "dogs-20240914.csv"),
                ("pets_dev", "dogs/size/heavy/", "dogs-20241015.csv"),
                ("pets_dev", "dogs/size/heavy/", "dogs-20241019.csv"),
                ("pets_dev", "dogs/size/heavy/", "dogs-20241021.csv"),
                ("pets_dev", "horses/europe/", "horses-20210219.csv"),
                ("pets_dev", "non-existent-prefix/", None),
            ],
            names=["bucket", "prefix", "name"],
        )
        df = Df(index=old_multi_index)
        s3_uris_map_df = Df(
            {
                "aws_account_1_pro": {
                    0: "s3://cars/europe/spain/",
                    1: "s3://pets/dogs/big_size/",
                    2: "s3://pets/horses/europe/",
                    3: "s3://pets/non-existent-prefix/",
                },
                "aws_account_3_dev": {
                    0: "s3://cars_dev/europe/spain/",
                    1: "s3://pets_dev/dogs/size/heavy/",
                    2: "s3://pets_dev/horses/europe/",
                    3: "s3://pets_dev/non-existent-prefix/",
                },
            }
        )
        result = m_s3_data._S3UriDfModifier(
            "aws_account_1_pro", "aws_account_3_dev", df
        )._get_df_modify_buckets_and_paths(s3_uris_map_df)
        expected_result = Df(
            index=MultiIndex.from_tuples(
                [
                    ("cars", "europe/spain/", "cars-20241014.csv"),
                    ("pets", "dogs/big_size/", "dogs-20240914.csv"),
                    ("pets", "dogs/big_size/", "dogs-20241015.csv"),
                    ("pets", "dogs/big_size/", "dogs-20241019.csv"),
                    ("pets", "dogs/big_size/", "dogs-20241021.csv"),
                    ("pets", "horses/europe/", "horses-20210219.csv"),
                    ("pets", "non-existent-prefix/", None),
                ],
                names=["bucket", "prefix", "name"],
            )
        )
        assert_frame_equal(expected_result, result)

    def test_get_new_multi_index_as_tuple_if_prefixes_end_or_not_with_slash(self):
        aws_account_origin = "pro"
        aws_account_target = "release"
        old_multi_index_as_tuple_prefix_ends_with_slash = "foo", "bar/baz/", "qux.txt"
        old_multi_index_as_tuple_prefix_does_not_end_with_slash = "foo", "bar/baz", "qux.txt"
        s3_uris_map_df_prefix_ends_with_slash = Df(
            {"pro": ["s3://foo/bar/", "s3://foo/bar/baz/"], "release": ["s3://foo/bar/", "s3://foo/bar/baz/"]}
        )
        s3_uris_map_df_prefix_does_not_end_with_slash = Df(
            {"pro": ["s3://foo/bar", "s3://foo/bar/baz"], "release": ["s3://foo/bar", "s3://foo/bar/baz"]}
        )
        for test_data in (
            (
                old_multi_index_as_tuple_prefix_ends_with_slash,
                s3_uris_map_df_prefix_ends_with_slash,
            ),
            (
                old_multi_index_as_tuple_prefix_ends_with_slash,
                s3_uris_map_df_prefix_does_not_end_with_slash,
            ),
            (
                old_multi_index_as_tuple_prefix_does_not_end_with_slash,
                s3_uris_map_df_prefix_ends_with_slash,
            ),
            (
                old_multi_index_as_tuple_prefix_does_not_end_with_slash,
                s3_uris_map_df_prefix_does_not_end_with_slash,
            ),
        ):
            old_multi_index_as_tuple, s3_uris_map_df = test_data
            with self.subTest(old_multi_index_as_tuple=old_multi_index_as_tuple, s3_uris_map_df=s3_uris_map_df):
                self.assertEqual(
                    ("foo", "bar/baz/", "qux.txt"),
                    m_s3_data._S3UriDfModifier(
                        aws_account_origin, aws_account_target, Df()
                    )._get_new_multi_index_as_tuple(old_multi_index_as_tuple, s3_uris_map_df),
                )
