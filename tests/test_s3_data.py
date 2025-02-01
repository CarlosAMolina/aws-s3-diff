import unittest

from pandas import DataFrame as Df
from pandas.testing import assert_frame_equal

from src import s3_data as m_s3_data

ExpectedResult = list[dict]

_ACCOUNT_ORIGIN = "pro"
_ACCOUNT_TARGET = "dev"


class TestS3UriDfModifier(unittest.TestCase):
    def test_get_df_set_s3_uris_in_origin_account_if_prefixes_end_and_not_end_with_slash(self):
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
        for test_data in self._get_all_combinations_for_df_and_map_df_with_and_without_trailing_slash():
            df, s3_uris_map_df = test_data
            with self.subTest(df=df, s3_uris_map_df=s3_uris_map_df):
                assert_frame_equal(
                    expected_result,
                    m_s3_data._S3UriDfModifier(
                        _ACCOUNT_ORIGIN, _ACCOUNT_TARGET, df
                    )._get_df_set_s3_uris_in_origin_account(s3_uris_map_df),
                )

    def _get_all_combinations_for_df_and_map_df_with_and_without_trailing_slash(self) -> list[tuple[Df, Df]]:
        return [
            (df, s3_uris_map_df)
            for df in (
                _AccountS3DataDfFactory().get_df_with_trailing_slash_in_prefix(),
                _AccountS3DataDfFactory().get_df_without_trailing_slash_in_prefix(),
            )
            for s3_uris_map_df in (
                _S3UrisMapDfFactory().get_df_with_trailing_slash(),
                _S3UrisMapDfFactory().get_df_without_trailing_slash(),
            )
        ]


def _get_df_as_multi_index(df: Df) -> Df:
    result = df.copy()
    result = result.set_index(["bucket", "prefix", "name"])
    result.columns = [[_ACCOUNT_TARGET] * len(result.columns), result.columns]
    return result


class _AccountS3DataDfFactory:
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

    def get_df_with_trailing_slash_in_prefix(self) -> Df:
        self._df["prefix"] = self._df["prefix"] + "/"
        self._with_multi_index()
        return self._df

    def get_df_without_trailing_slash_in_prefix(self) -> Df:
        self._with_multi_index()
        return self._df

    def _with_multi_index(self) -> "_AccountS3DataDfFactory":
        self._df = _get_df_as_multi_index(self._df)
        return self


class _S3UrisMapDfFactory:
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

    def get_df_without_trailing_slash(self) -> Df:
        return self._df

    def get_df_with_trailing_slash(self) -> Df:
        for account in (_ACCOUNT_ORIGIN, _ACCOUNT_TARGET):
            self._df[account] = self._df[account] + "/"
        return self._df
