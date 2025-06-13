class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class EmptyAwsAccountNameAnalysisConfigError(ValueError):
    pass
