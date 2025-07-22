import unittest
from unittest import mock

import numpy as np
from pandas import DataFrame as Df
from pandas.testing import assert_frame_equal

from aws_s3_diff.s3_data.one_account import AccountDataGenerator
from aws_s3_diff.s3_data.one_account import OriginS3UrisAsIndexAccountDfModifier
from aws_s3_diff.type_custom import S3Query

ExpectedResult = list[dict]

_ACCOUNT_ORIGIN = "pro"
_ACCOUNT_TARGET = "dev"


class TestAccountDataGenerator(unittest.TestCase):
    @mock.patch("aws_s3_diff.s3_data.one_account.S3Client")
    @mock.patch("aws_s3_diff.s3_data.one_account.S3UrisFileReader")
    def test_get_df_returns_df_with_bucket_and_prefix_values_if_query_without_results(
        self,
        mock_s3_uris_file_reader,
        mock_s3_client,
    ):
        mock_s3_uris_file_reader().get_s3_queries_for_account.return_value = [
            S3Query("bucket_1", "prefix_1"),
            S3Query("bucket_2", "prefix_2"),
        ]
        mock_s3_client().get_s3_data.return_value = []
        expected_result = Df(
            data=[
                ["bucket_1", "prefix_1/", np.nan, np.nan, np.nan, np.nan],
                ["bucket_2", "prefix_2/", np.nan, np.nan, np.nan, np.nan],
            ],
            columns=["bucket", "prefix", "name", "date", "size", "hash"],
            index=[0, 0],
        )
        result = AccountDataGenerator("foo").get_df()
        assert_frame_equal(expected_result, result)


class TestOriginS3UrisAsIndexAccountDfModifier(unittest.TestCase):
    def test_get_df_replace_index_with_s3_uris_map_if_prefixes_end_and_not_end_with_slash(self):
        expected_result = _get_df_as_multi_index(
            Df(
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
        )
        for df, s3_uris_map_df in self._get_all_combinations_for_df_and_map_df_with_and_without_trailing_slash():
            with self.subTest(df=df, s3_uris_map_df=s3_uris_map_df):
                assert_frame_equal(
                    expected_result,
                    OriginS3UrisAsIndexAccountDfModifier(
                        _ACCOUNT_ORIGIN, _ACCOUNT_TARGET
                    )._get_df_replace_index_with_s3_uris_map(df, s3_uris_map_df),
                )

    def _get_all_combinations_for_df_and_map_df_with_and_without_trailing_slash(self) -> list[tuple[Df, Df]]:
        df = _AccountS3DataDfBuilder().with_trailing_slash_in_prefix().build()
        return [
            (df, s3_uris_map_df)
            for s3_uris_map_df in (
                _S3UrisMapDfBuilder().with_trailing_slash().build(),
                _S3UrisMapDfBuilder().without_trailing_slash().build(),
            )
        ]


def _get_df_as_multi_index(df: Df) -> Df:
    result = df.copy()
    result = result.set_index(["bucket", "prefix", "name"])
    result.columns = [[_ACCOUNT_TARGET] * len(result.columns), result.columns]
    return result


class _AccountS3DataDfBuilder:
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

    def build(self) -> Df:
        return self._df

    def with_trailing_slash_in_prefix(self) -> "_AccountS3DataDfBuilder":
        self._df["prefix"] = self._df["prefix"] + "/"
        self._with_multi_index()
        return self

    def _with_multi_index(self) -> "_AccountS3DataDfBuilder":
        self._df = _get_df_as_multi_index(self._df)
        return self


class _S3UrisMapDfBuilder:
    def __init__(self):
        self._df = Df(
            {
                _ACCOUNT_ORIGIN: {
                    0: "s3://cars/europe/spain",
                    1: "s3://pets/dogs/big_size",
                    2: "s3://pets/horses/europe",
                    3: "s3://pets/non-existent-prefix",
                },
                _ACCOUNT_TARGET: {
                    0: "s3://cars_dev/europe/spain",
                    1: "s3://pets_dev/dogs/size/heavy",
                    2: "s3://pets_dev/horses/europe",
                    3: "s3://pets_dev/non-existent-prefix",
                },
            }
        )

    def build(self) -> Df:
        return self._df

    def with_trailing_slash(self) -> "_S3UrisMapDfBuilder":
        for account in (_ACCOUNT_ORIGIN, _ACCOUNT_TARGET):
            self._df[account] = self._df[account] + "/"
        return self

    def without_trailing_slash(self) -> "_S3UrisMapDfBuilder":
        return self
