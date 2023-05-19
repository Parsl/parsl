import dill
import pickle
import logging

logger = logging.getLogger(__name__)
from parsl.serialize.base import SerializerBase

from typing import Any

# parsl/serialize/concretes.py:10: error: Module "proxystore.store" does not explicitly export attribute "Store"  [attr-defined]
from proxystore.store import Store, get_store, register_store  # type: ignore

from proxystore.connectors.file import FileConnector
store = Store(name='parsl_store', connector=FileConnector(store_dir="/tmp"))
register_store(store)


class ProxyStoreSerializer(SerializerBase):
    _identifier = b'99\n'
    _for_code = False
    _for_data = True

    def serialize(self, data: Any) -> bytes:

        store = get_store("parsl_store")
        assert store is not None, "Could not find store"

        p = store.proxy(data)

        d = pickle.dumps(p)

        return self.identifier + d

    def deserialize(self, payload: bytes) -> Any:
        chomped = self.chomp(payload)
        proxy = pickle.loads(chomped)
        return proxy


class PickleSerializer(SerializerBase):
    """ Pickle serialization covers most python objects, with some notable exceptions:

    * functions defined in a interpreter/notebook
    * classes defined in local context and not importable using a fully qualified name
    * closures, generators and coroutines
    * [sometimes] issues with wrapped/decorated functions
    """

    _identifier = b'01\n'
    _for_code = True
    _for_data = True

    def serialize(self, data: Any) -> bytes:
        x = pickle.dumps(data)
        return self.identifier + x

    def deserialize(self, payload: bytes) -> Any:
        chomped = self.chomp(payload)
        data = pickle.loads(chomped)
        return data


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

    _identifier = b'02\n'
    _for_code = True
    _for_data = True

    def serialize(self, data: Any) -> bytes:
        x = dill.dumps(data)
        return self.identifier + x

    def deserialize(self, payload: bytes) -> Any:
        chomped = self.chomp(payload)
        data = dill.loads(chomped)
        return data
