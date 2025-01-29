import unittest

from pandas import DataFrame as Df
from pandas.testing import assert_frame_equal

from src import s3_data as m_s3_data

ExpectedResult = list[dict]


class TestS3UriDfModifier(unittest.TestCase):
    def test_get_df_set_s3_uris_in_origin_account_if_prefixes_end_and_not_end_with_slash(self):
        aws_account_origin = "aws_account_1_pro"
        aws_account_target = "aws_account_3_dev"
        df_prefix_does_not_end_with_slash = _AwsAccountS3DataDfBuilder().without_trailing_slash_in_prefix().build()
        df_prefix_ends_with_slash = df_prefix_does_not_end_with_slash.copy()
        df_prefix_ends_with_slash["prefix"] = df_prefix_ends_with_slash["prefix"] + "/"
        df_prefix_ends_with_slash = _get_df_as_multi_index(aws_account_target, df_prefix_ends_with_slash)
        df_prefix_does_not_end_with_slash = _get_df_as_multi_index(
            aws_account_target, df_prefix_does_not_end_with_slash
        )
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
        expected_result = _get_df_as_multi_index(aws_account_target, expected_result)
        for test_data in (
            (
                df_prefix_ends_with_slash,
                s3_uris_map_df_prefix_ends_with_slash,
            ),
            (
                df_prefix_ends_with_slash,
                s3_uris_map_df_prefix_does_not_end_with_slash,
            ),
            (
                df_prefix_does_not_end_with_slash,
                s3_uris_map_df_prefix_ends_with_slash,
            ),
            (
                df_prefix_does_not_end_with_slash,
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


def _get_df_as_multi_index(aws_account_target: str, df: Df) -> Df:
    result = df.copy()
    result = result.set_index(["bucket", "prefix", "name"])
    result.columns = [[aws_account_target] * len(result.columns), result.columns]
    return result


class _AwsAccountS3DataDfBuilder:
    def __init__(self):
        self._df = Df(
            [
                ["cars_dev", "europe/spain", "cars-20241014.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy", "dogs-20240914.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy", "dogs-20241015.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy", "dogs-20241019.csv"] + ["foo"] * 3,
                ["pets_dev", "dogs/size/heavy", "dogs-20241021.csv"] + ["foo"] * 3,
                ["pets_dev", "horses/europe", "horses-20210219.csv"] + ["foo"] * 3,
                ["pets_dev", "non-existent-prefix", None] + ["foo"] * 3,
            ],
            columns=["bucket", "prefix", "name", "date", "size", "hash"],
        )

    def without_trailing_slash_in_prefix(self) -> "_AwsAccountS3DataDfBuilder":
        return self

    def build(self) -> Df:
        return self._df
