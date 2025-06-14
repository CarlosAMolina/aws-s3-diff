class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class S3UrisFileError(ValueError):
    pass


class DuplicatedUriS3UrisFileError(S3UrisFileError):
    pass


class EmptyAccountNameS3UrisFileError(S3UrisFileError):
    def __init__(self):
        message = "Some AWS account names are empty (file s3-uris-to-analyze.csv)"
        super().__init__(message)


class EmptyUriS3UrisFileError(S3UrisFileError):
    pass
