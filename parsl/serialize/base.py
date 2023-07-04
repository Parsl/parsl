from abc import abstractmethod, ABCMeta

from typing import Any


class SerializerBase(metaclass=ABCMeta):
    """ Adds shared functionality for all serializer implementations
    """

    # For deserializer
    _identifier: bytes

    # For serializer
    _for_code: bool
    _for_data: bool

    # For deserializer
    @property
    def identifier(self) -> bytes:
        """Get that identifier that will be used to indicate in byte streams
        that this class should be used for deserialization.

        Returns
        -------
        identifier : bytes
        """
        return self._identifier

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, payload: bytes) -> Any:
        pass
