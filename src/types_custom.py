from typing import NamedTuple

from pandas import DataFrame as Df


class S3Query(NamedTuple):
    bucket: str
    prefix: str

    def __repr__(self):
        return f"s3://{self.bucket}/{self.prefix}"


class FileS3Data(NamedTuple):
    name: str | None = None
    date: str | None = None
    size: int | None = None
    hash: str | None = None


S3Data = list[FileS3Data]
AllAccountsS3DataDf = Df  # It is the combination of all AWS accounts S3 data.
AnalysisS3DataDf = Df  # It is the AllAccountsS3DataDf plus the analysis result columns.
