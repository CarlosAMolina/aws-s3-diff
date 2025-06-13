class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class EmptyAccountNameAnalysisConfigError(ValueError):
    pass


class EmptyUriAnalysisConfigError(ValueError):
    pass


class DuplicatedUriAnalysisConfigError(ValueError):
    pass
