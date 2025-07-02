import re

from pandas import DataFrame as Df
from pandas import Series

# S3 uri regex: https://stackoverflow.com/a/47130367
_REGEX_BUCKET_PREFIX_FROM_S3_URI = r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)"


def get_df_add_last_slash_to_values(df: Df, column_name: str) -> Df:
    result = df
    result.loc[~result[column_name].str.endswith("/"), column_name] = result[column_name] + "/"
    return result


def get_df_uri_parts(series: Series) -> Df:
    return series.str.extract(_REGEX_BUCKET_PREFIX_FROM_S3_URI, expand=False)


class S3UriParts:
    def __init__(self, s3_uri: str):
        self._s3_uri = s3_uri

    @property
    def bucket(self) -> str:
        return self._get_regex_match_s3_uri_parts(self._s3_uri).group("bucket_name")

    @property
    def key(self) -> str:
        return self._get_regex_match_s3_uri_parts(self._s3_uri).group("object_key")

    def _get_regex_match_s3_uri_parts(self, s3_uri: str) -> re.Match:
        result = re.match(_REGEX_BUCKET_PREFIX_FROM_S3_URI, s3_uri)
        assert result is not None
        return result
