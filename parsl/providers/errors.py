import warnings

from parsl.errors import ParslError


class ExecutionProviderException(ParslError):
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

    def __str__(self):
        return "SchedulerMissingArgs: Pool:{0} Arg:{1}".format(self.sitename, self.missing_keywords)


class ScriptPathError(ExecutionProviderException):
    ''' Error raised when the template used to compose the submit script to the local resource manager is missing required arguments
    '''

    def __init__(self, script_path, reason):
        self.script_path = script_path
        self.reason = reason

    def __str__(self):
        return "Unable to write submit script:{0} Reason:{1}".format(self.script_path, self.reason)


class SubmitException(ExecutionProviderException):
    '''Raised by the submit() method of a provider if there is an error in launching a job.
    '''

    def __init__(self, job_name, message, stdout=None, stderr=None, retcode=None):
        self.job_name = job_name
        self.message = message
        self.stdout = stdout
        self.stderr = stderr
        self.retcode = retcode

    @property
    def task_name(self) -> str:
        warnings.warn("task_name is deprecated; use .job_name instead. This will be removed after 2024-06.", DeprecationWarning)
        return self.job_name

    def __str__(self) -> str:
        # TODO: make this more user-friendly
        return f"Cannot launch job {self.job_name}: {self.message}; recode={self.retcode}, stdout={self.stdout}, stderr={self.stderr}"
