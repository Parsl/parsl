class ParslError(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    pass


class NotFutureError(ParslError):
    ''' Basically a type error. A non future item was passed to a function
    that expected a future.
    '''
    pass

class InvalidAppTypeError(ParslError):
    ''' An invalid app type was requested from the the @App decorator.
    '''
    pass


class AppException(ParslError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    '''
    pass
