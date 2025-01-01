from typing import NamedTuple

from pandas import DataFrame as Df


class S3Query(NamedTuple):
    bucket: str
    prefix: str

    def __repr__(self):
        return f"s3://{self.bucket}/{self.prefix}"


class FileS3Data(NamedTuple):
    name: str
    date: str
    size: int


S3Data = list[dict[str, str | int]]  # TODO replace with list[FileS3Data]
AllAccoutsS3DataDf = Df  # It is the combination of all AWS accounts S3 data.
AnalysisS3DataDf = Df  # It is the AllAccoutsS3DataDf plus the analysis result columns.
