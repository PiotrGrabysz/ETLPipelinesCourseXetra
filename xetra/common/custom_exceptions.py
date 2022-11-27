"""
Custom Exceptions
"""


class WrongFormatException(Exception):
    """
    WrongFormatException class.

    Exception can be raised when the format type is not supported, e.g. by the S3BucketConnector.
    """


class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class.

    Exception is raised when columns of an already existing meta file does not match columns of a new meta file.
    """