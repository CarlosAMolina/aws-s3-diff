class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class S3UrisFileError(ValueError):
    pass


class DuplicatedUriS3UrisFileError(S3UrisFileError):
    pass


class EmptyAccountNameS3UrisFileError(S3UrisFileError):
    _message = "Some AWS account names are empty (file s3-uris-to-analyze.csv)"

    def __init__(self, *args):
        super().__init__(self._message)


class EmptyUriS3UrisFileError(S3UrisFileError):
    pass
