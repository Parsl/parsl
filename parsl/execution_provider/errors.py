class ProviderFactoryError(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    def __repr__(self):
        return "Pool:{0}, Reason:{1}".format(self.pool, self.reason)

    def __str__(self):
        return self.__repr__()


class BadConfig(ProviderFactoryError):
    """ The user provided a bad config

    Contains:
        - Reason
        - Pool name
    """

    def __init__ (self, pool, reason):
        super().__init__()
        self.pool = pool
        self.reason = reason
