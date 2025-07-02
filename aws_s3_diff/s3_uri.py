import re

# S3 uri regex: https://stackoverflow.com/a/47130367
REGEX_BUCKET_PREFIX_FROM_S3_URI = r"s3://(?P<bucket_name>.+?)/(?P<object_key>.+)"


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
        result = re.match(REGEX_BUCKET_PREFIX_FROM_S3_URI, s3_uri)
        assert result is not None
        return result
