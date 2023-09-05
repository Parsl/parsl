from parsl.errors import ParslError


class DeserializationError(ParslError):
    """Failure at the deserialization of results/exceptions from remote workers.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __str__(self) -> str:
        return f"Failed to deserialize objects. Reason: {self.reason}"


class SerializationError(ParslError):
    """Failure to serialize task objects.
    """

    def __init__(self, fname: str) -> None:
        self.fname = fname
        self.troubleshooting = "https://parsl.readthedocs.io/en/latest/faq.html#addressing-serializationerror"

    def __str__(self) -> str:
        return f"Failed to serialize objects for an invocation of function {self.fname}. Refer {self.troubleshooting}"


class DeserializerPluginError(ParslError):
    """Failure to dynamically load a deserializer plugin.
    """

    def __init__(self, header: bytes) -> None:
        self.header = header

    def __str__(self) -> str:
        return f"Failed to load deserializer plugin for header {self.header!r}"
