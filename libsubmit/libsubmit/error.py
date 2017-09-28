class ExecutionProviderExceptions(Exception):
    """ Base class for all exceptions
    Only to be invoked when only a more specific error is not available.

    """
    pass

class SchedulerMissingArgs(ExecutionProviderExceptions):
    ''' Error raised when the template used to compose the submit script to the local resource manager is missing required arguments
    '''
    def __init__ (self, missing_keywords, sitename):
        self.missing_keywords = missing_keywords
        self.sitename = sitename

    def __repr__ (self):
        return "SchedulerMissingArgs: Pool:{0} Arg:{1}".format(self.sitename, self.missing_keywords)


class ScriptPathError(ExecutionProviderExceptions):
    ''' Error raised when the template used to compose the submit script to the local resource manager is missing required arguments
    '''
    def __init__ (self, script_path, reason):
        self.script_path = script_path
        self.reason = reason

    def __repr__ (self):
        return "Unable to write submit script:{0} Reason:{1}".format(self.script_path,
                                                                     self.reason)

