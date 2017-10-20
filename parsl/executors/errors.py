''' Exceptions raise by Executors.
'''

class ExecutorError(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    def __repr__(self):
        return "Site:{0}, Reason:{1}".format(self.site, self.reason)

    def __str__(self):
        return self.__repr__()


class ScalingFailed(ExecutorError):
    ''' Scaling failed due to error in Execution provider.
    '''
    def __init__ (self, sitename, reason):
        self.site = sitename
        self.reason = reason
