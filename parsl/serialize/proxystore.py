import dill
import io
import typing as t

from parsl.serialize.base import SerializerBase
from proxystore.store import Store


class ProxyStoreDeepPickler(dill.Pickler):
    """This class extends dill so that certain objects will be stored into
    ProxyStore rather than serialized directly. The selection of objects is
    made by a user-specified policy.
    """

    def __init__(self, *args: t.Any, should_proxy: t.Callable[[t.Any], bool], store: Store, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._store = store
        self._should_proxy = should_proxy

    def reducer_override(self, o: t.Any) -> t.Any:
        if self._should_proxy(o):
            proxy = self._store.proxy(o)
            return proxy.__reduce__()
        else:
            # fall through to dill
            return NotImplemented


class ProxyStoreSerializer(SerializerBase):

    def __init__(self, *, should_proxy: t.Optional[t.Callable[[t.Any], bool]] = None, store: t.Optional[Store] = None) -> None:
        self._store = store
        self._should_proxy = should_proxy

    def serialize(self, data: t.Any) -> bytes:
        assert self._store is not None
        assert self._should_proxy is not None

        assert data is not None

        f = io.BytesIO()
        pickler = ProxyStoreDeepPickler(file=f, store=self._store, should_proxy=self._should_proxy)
        pickler.dump(data)
        return f.getvalue()

    def deserialize(self, body: bytes) -> t.Any:
        # because we aren't customising deserialization, use regular
        # dill for deserialization
        return dill.loads(body)
