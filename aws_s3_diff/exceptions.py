class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class S3UrisFileError(ValueError):
    pass


class DuplicatedUriS3UrisFileError(S3UrisFileError):
    pass


class EmptyAccountNameS3UrisFileError(S3UrisFileError):
    pass


class EmptyUriS3UrisFileError(S3UrisFileError):
    pass
