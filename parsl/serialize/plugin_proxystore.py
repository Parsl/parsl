# parsl/serialize/concretes.py:10: error: Module "proxystore.store" does not explicitly export attribute "Store"  [attr-defined]
from proxystore.store import Store, register_store
from proxystore.connectors.file import FileConnector
from parsl.serialize.facade import register_method_for_data

from parsl.serialize.base import SerializerBase

from typing import Any, Optional

import pickle


class ProxyStoreSerializer(SerializerBase):

    def __init__(self, store: Optional[Store] = None) -> None:
        """Because of jumbled use of this class for init-time configurable
        serialization, and non-configurable remote deserializer loading, the
        store field can be None... TODO: this would go away if serializer and
        deserializer were split into different objects/classes/functions."""
        self._store = store

    def serialize(self, data: Any) -> bytes:
        assert self._store is not None
        assert data is not None
        p = self._store.proxy(data)
        return pickle.dumps(p)

    def deserialize(self, body: bytes) -> Any:
        return pickle.loads(body)


def register_proxystore_serializer() -> None:
    """Initializes proxystore and registers it as a serializer with parsl"""
    serializer = create_proxystore_serializer()
    register_method_for_data(serializer)


def create_proxystore_serializer() -> ProxyStoreSerializer:
    """Creates a serializer but does not register with global system - so this
    can be used in testing."""

    import uuid
    store = Store(name='parsl_store_' + str(uuid.uuid4()), connector=FileConnector(store_dir="/tmp"))
    register_store(store)
    return ProxyStoreSerializer(store)
