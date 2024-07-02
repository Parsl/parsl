# parsl/serialize/concretes.py:10: error: Module "proxystore.store" does not explicitly export attribute "Store"  [attr-defined]
import io
import logging
from typing import Any, Optional, Type

import dill
from proxystore.connectors.file import FileConnector
from proxystore.store import Store, register_store

from parsl.serialize.base import SerializerBase

logger = logging.getLogger(__name__)


class ProxyStoreDeepPickler(dill.Pickler):

    def __init__(self, *args: Any, policy: Type, store: Store, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._store = store
        self._policy = policy

    def reducer_override(self, o: Any) -> Any:
        logger.info(f"BENC: reducing object {o!r}")

        if type(o) is self._policy:  # not isinstance, because want exact class match
            logger.info("BENC: Policy class detected")
            proxy = self._store.proxy(o)
            return proxy.__reduce__()
        else:
            # fall through to pickle...
            return NotImplemented


class ProxyStoreDeepSerializer(SerializerBase):

    def __init__(self, *, policy: Optional[Type] = None, store: Optional[Store] = None) -> None:
        """Because of jumbled use of this class for init-time configurable
        serialization, and non-configurable remote deserializer loading, the
        store and policy fields can be None... TODO: this would go away if serializer and
        deserializer were split into different objects/classes/functions, like Pickler and
        Unpickler are"""
        self._store = store
        self._policy = policy

    def serialize(self, data: Any) -> bytes:
        assert self._store is not None
        assert self._policy is not None

        assert data is not None

        # TODO: pluggable policy should go here... what does that look like?
        # TODO: this policy belongs in the pickler plugin, not top level parsl serializer plugin
        # if not isinstance(data, int):
        #    raise RuntimeError(f"explicit policy will only proxy ints, not {type(data)}")

        f = io.BytesIO()
        pickler = ProxyStoreDeepPickler(file=f, store=self._store, policy=self._policy)
        pickler.dump(data)
        return f.getvalue()

    def deserialize(self, body: bytes) -> Any:
        # because we aren't customising deserialization, use regular
        # dill for deserialization; but otherwise could create a
        # custom Unpickler here...
        return dill.loads(body)


def create_deep_proxystore_serializer(*, policy: Type) -> ProxyStoreDeepSerializer:
    """Creates a serializer but does not register with global system - so this
    can be used in testing."""

    store = Store(name='parsl_store', connector=FileConnector(store_dir="/tmp"))
    register_store(store)
    return ProxyStoreDeepSerializer(store=store, policy=policy)
