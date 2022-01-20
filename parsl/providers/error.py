class ExecutionProviderException(Exception):
    """ Base class for all exceptions
    Only to be invoked when only a more specific error is not available.

    """
    pass


class ScaleOutFailed(ExecutionProviderException):
    ''' Scale out failed in the submit phase on the provider side
    '''

    def __init__(self, provider, reason):
        self.provider = provider
        self.reason = reason

    def __str__(self):
        return "Unable to scale out {} provider: {}".format(self.provider, self.reason)


class SchedulerMissingArgs(ExecutionProviderException):
    ''' Error raised when the template used to compose the submit script to the local resource manager is missing required arguments
    '''

    def __init__(self, missing_keywords, sitename):
        self.missing_keywords = missing_keywords
        self.sitename = sitename

    def __repr__(self):
        return "SchedulerMissingArgs: Pool:{0} Arg:{1}".format(self.sitename, self.missing_keywords)


class ScriptPathError(ExecutionProviderException):
    ''' Error raised when the template used to compose the submit script to the local resource manager is missing required arguments
    '''

    def __init__(self, script_path, reason):
        self.script_path = script_path
        self.reason = reason

    def __repr__(self):
        return "Unable to write submit script:{0} Reason:{1}".format(self.script_path, self.reason)


class SubmitException(ExecutionProviderException):
    '''Raised by the submit() method of a provider if there is an error in launching a task.
    '''

    def __init__(self, task_name, message, stdout=None, stderr=None):
        self.task_name = task_name
        self.message = message
        self.stdout = stdout
        self.stderr = stderr

    def __repr__(self):
        # TODO: make this more user-friendly
        return "Cannot launch task {0}: {1}; stdout={2}, stderr={3}".format(self.task_name,
                                                                            self.message,
                                                                            self.stdout,
                                                                            self.stderr)
