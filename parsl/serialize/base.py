from abc import abstractmethod, ABCMeta

from typing import Any


class SerializerBase(metaclass=ABCMeta):
    """ Adds shared functionality for all serializer implementations
    """

    _identifier: bytes

    @property
    def identifier(self) -> bytes:
        """Get that identifier that will be used to indicate in byte streams
        that this class should be used for deserialization.

        TODO: for user derived serialisers, this should be fixed to be the
        appropriate module and class name so that it can be loaded dynamically:
        a serializer shouldn't be forced to specify an _identifier unless its
        trying to short-cut that path.

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
