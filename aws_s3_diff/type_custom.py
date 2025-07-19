from typing import NamedTuple


class S3Query:
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self._prefix = prefix

    def __repr__(self):
        return f"s3://{self.bucket}/{self.prefix}"

    def __eq__(self, other):
        if isinstance(other, S3Query):
            return self.bucket == other.bucket and self.prefix == other.prefix
        return False

    def __hash__(self):
        return hash((self.bucket, self._prefix))

    @property
    def prefix(self) -> str:
        # If a S3 query does not end in slash, S3 folders are not managed correctly.
        return self._prefix if self._prefix.endswith("/") else f"{self._prefix}/"


class FileS3Data(NamedTuple):
    name: str | None = None
    date: str | None = None
    size: int | None = None
    hash: str | None = None


S3Data = list[FileS3Data]
