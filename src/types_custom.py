from collections import namedtuple

S3Query = namedtuple("S3Query", "bucket prefix")
_S3FileData = dict
S3Data = list[_S3FileData]
