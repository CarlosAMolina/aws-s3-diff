class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class DuplicatedUriS3UrisFileError(ValueError):
    pass


class EmptyAccountNameS3UrisFileError(ValueError):
    pass


class EmptyUriS3UrisFileError(ValueError):
    pass
