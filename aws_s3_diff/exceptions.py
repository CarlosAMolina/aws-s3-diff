class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class S3UrisFileError(ValueError):
    def __init__(self, **kwargs):
        super().__init__(self._message.format(**kwargs))


class DuplicatedUriS3UrisFileError(S3UrisFileError):
    _message = "Error in s3-uris-to-analyze.csv. The AWS account {account} has duplicated URIs"


class EmptyAccountNameS3UrisFileError(S3UrisFileError):
    _message = "Error in s3-uris-to-analyze.csv. Some AWS account names are empty"


class EmptyUriS3UrisFileError(S3UrisFileError):
    _message = "Error in s3-uris-to-analyze.csv. Some URIs are empty"
