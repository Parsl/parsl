from parsl.errors import ParslError


class SerializationError(ParslError):
    """Failure to serialize task objects.
    """

    def __init__(self, fname: str) -> None:
        self.fname = fname
        self.troubleshooting = "https://parsl.readthedocs.io/en/latest/faq.html#addressing-serializationerror"

    def __str__(self) -> str:
        return "Failed to serialize objects for an invocation of function {}. Refer {} ".format(self.fname,
                                                                                                self.troubleshooting)
