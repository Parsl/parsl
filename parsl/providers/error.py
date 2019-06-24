class ExecutionProviderException(Exception):
    """ Base class for all exceptions
    Only to be invoked when only a more specific error is not available.

    """
    pass


class OptionalModuleMissing(ExecutionProviderException):
    ''' Error raised a required module is missing for a optional/extra provider
    '''

    def __init__(self, module_names, reason):
        self.module_names = module_names
        self.reason = reason

    def __repr__(self):
        return "Unable to Initialize provider.Missing:{0},  Reason:{1}".format(
            self.module_names, self.reason
        )


class ChannelRequired(ExecutionProviderException):
    ''' Execution provider requires a channel.
    '''

    def __init__(self, provider, reason):
        self.provider = provider
        self.reason = reason

    def __repr__(self):
        return "Unable to Initialize provider.Provider:{0}, Reason:{1}".format(
            self.provider, self.reason
        )


class ScaleOutFailed(ExecutionProviderException):
    ''' Generic catch. Scale out failed in the submit phase on the provider side
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
