from abc import abstractmethod, ABCMeta
from functools import cached_property
from typing import Any


class SerializerBase(metaclass=ABCMeta):
    """ Adds shared functionality for all serializer implementations
    """

    @cached_property
    def identifier(self) -> bytes:
        """Compute the identifier that will be used to indicate in byte streams
        that this class should be used for deserialization.

        Classes that wish to use a self-managed identifier namespace, such as
        the default concretes.py implementations, should override this property
        with their own identifier.

        Returns
        -------
        identifier : bytes
        """
        t = type(self)
        m = bytes(t.__module__, encoding="utf-8")
        c = bytes(t.__name__, encoding="utf-8")
        return m + b' ' + c

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, payload: bytes) -> Any:
        pass
