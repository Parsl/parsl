''' Exceptions raise by Apps.
'''
from parsl.errors import ParslError


class FileCopyException(ParslError):
    ''' File copy operation failed

    Contains:
    e (parent exception object)
    '''

    def __init__(self, e: Exception) -> None:
        super().__init__("File copy failed due to {0}".format(e))
