from typing import List


class ParslError(Exception):
    """Base class for all exceptions.

    Only to be invoked when a more specific error is not available.
    """


class ConfigurationError(ParslError):
    """Raised when a component receives an invalid configuration.
    """


class OptionalModuleMissing(ParslError):
    ''' Error raised when a required module is missing for a optional/extra component
    '''

    def __init__(self, module_names: List[str], reason: str):
        self.module_names = module_names
        self.reason = reason

    def __str__(self) -> str:
        return "The functionality requested requires optional modules {0} which could not be imported, because: {1}".format(
            self.module_names, self.reason
        )


class InternalConsistencyError(ParslError):
    """Raised when a component enounters an internal inconsistency.
    """


class NoDataFlowKernelError(ParslError):
    """Raised when no DataFlowKernel is available for an operation that needs one.
    """
