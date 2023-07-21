import pytest
import uuid


def policy_example(o):
    """Example policy will proxy only lists."""
    return isinstance(o, list)


@pytest.mark.local
def test_proxystore_nonglobal():
    """Check that values are roundtripped, for both proxied and non-proxied types.
    """
    # import in function, because proxystore is not importable in base parsl
    # installation.
    from parsl.serialize.proxystore import ProxyStoreSerializer
    from proxystore.proxy import Proxy
    from proxystore.store import Store, register_store
    from proxystore.connectors.file import FileConnector

    store = Store(name='parsl_store_' + str(uuid.uuid4()), connector=FileConnector(store_dir="/tmp"))
    register_store(store)

    s = ProxyStoreSerializer(store=store, should_proxy=policy_example)

    # check roundtrip for an int, which will not be proxystored
    s_7 = s.serialize(7)
    assert isinstance(s_7, bytes)
    roundtripped_7 = s.deserialize(s_7)
    assert roundtripped_7 == 7
    assert not isinstance(roundtripped_7, Proxy)

    v = [1, 2, 3]
    s_v = s.serialize(v)
    assert isinstance(s_7, bytes)
    roundtripped_v = s.deserialize(s_v)
    assert roundtripped_v == v
    assert isinstance(roundtripped_v, Proxy)
