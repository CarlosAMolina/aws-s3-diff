from collections import namedtuple

from pandas import DataFrame as Df

S3Query = namedtuple("S3Query", "bucket prefix")
_S3FileData = dict
S3Data = list[_S3FileData]
AllAccoutsS3DataDf = Df
