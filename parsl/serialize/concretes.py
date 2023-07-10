import dill
import functools
import pickle
import logging

logger = logging.getLogger(__name__)
from parsl.serialize.base import SerializerBase

from typing import Any


class PickleSerializer(SerializerBase):
    """ Pickle serialization covers most python objects, with some notable exceptions:

    * functions defined in a interpreter/notebook
    * classes defined in local context and not importable using a fully qualified name
    * closures, generators and coroutines
    """

    _identifier = b'01'
    _for_code = False
    _for_data = True

    def serialize(self, data: Any) -> bytes:
        return pickle.dumps(data)

    def deserialize(self, body: bytes) -> Any:
        return pickle.loads(body)


class PickleCallableSerializer(SerializerBase):
    """This serializer is a variant of the PickleSerializer that will
    serialize and deserialize callables using an lru_cache, under the
    assumption that callables are immutable and so can be cached.
    """

    _identifier = b'C1'
    _for_code = True
    _for_data = False

    @functools.lru_cache
    def serialize(self, data: Any) -> bytes:
        return pickle.dumps(data)

    @functools.lru_cache
    def deserialize(self, body: bytes) -> Any:
        return pickle.loads(body)


class DillSerializer(SerializerBase):
    """ Dill serialization works on a superset of object including the ones covered by pickle.
    However for most cases pickle is faster. For most callable objects the additional overhead
    of dill can be amortized with an lru_cache. Here's items that dill handles that pickle
    doesn't:

    * functions defined in a interpreter/notebook
    * classes defined in local context and not importable using a fully qualified name
    * functions that are wrapped/decorated by other functions/classes
    * closures
    """

    _identifier = b'02'
    _for_code = False
    _for_data = True

    def serialize(self, data: Any) -> bytes:
        return dill.dumps(data)

    def deserialize(self, body: bytes) -> Any:
        return dill.loads(body)


class DillCallableSerializer(SerializerBase):
    """This serializer is a variant of the DillSerializer that will
    serialize and deserialize callables using an lru_cache, under the
    assumption that callables are immutable and so can be cached.
    """

    _identifier = b'C2'
    _for_code = True
    _for_data = False

    @functools.lru_cache
    def serialize(self, data: Any) -> bytes:
        return dill.dumps(data)

    @functools.lru_cache
    def deserialize(self, body: bytes) -> Any:
        return dill.loads(body)
