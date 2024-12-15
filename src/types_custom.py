from collections import namedtuple

from pandas import DataFrame as Df

S3Query = namedtuple("S3Query", "bucket prefix")
_S3FileData = dict
S3Data = list[_S3FileData]
AllAccoutsS3DataDf = Df  # It is the combination of all AWS accounts S3 data.
AnalysisS3DataDf = Df  # It is the AllAccoutsS3DataDf plus the analysis result columns.
