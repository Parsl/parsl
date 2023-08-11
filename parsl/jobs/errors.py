from parsl.errors import ParslError


class TooManyJobFailuresError(ParslError):
    """Indicates that executor is shut down because of too many block failures.
    """
    pass
