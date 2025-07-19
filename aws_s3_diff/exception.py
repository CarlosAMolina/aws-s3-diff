MESSAGE_INCORRECT_CREDENTIALS = "Incorrect AWS credentials. Authenticate and run the program again"


class FolderInS3UriError(IsADirectoryError):
    pass


class AnalysisConfigError(ValueError):
    pass


class S3UrisFileError(ValueError):
    _message = "Error in s3-uris-to-analyze.csv"
    _error_detail = "No error specified"

    def __init__(self, **kwargs):
        error_detail = self._error_detail.format(**kwargs)
        message = self._message + ". " + error_detail
        super().__init__(message)


class DuplicatedUriS3UrisFileError(S3UrisFileError):
    _error_detail = "The AWS account {account} has duplicated URIs"


class EmptyAccountNameS3UrisFileError(S3UrisFileError):
    _error_detail = "Some AWS account names are empty"


class EmptyUriS3UrisFileError(S3UrisFileError):
    _error_detail = "Some URIs are empty"
