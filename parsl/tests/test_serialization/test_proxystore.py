import pytest

# from parsl.serialize.facade import methods_for_data
from parsl.serialize.proxystore import ProxyStoreSerializer


def policy_example(o):
    """Example policy will proxy only lists."""
    return isinstance(o, list)

@pytest.mark.local
def test_proxystore_nonglobal():
    """Check that non-proxy-store values are roundtripped.
    """
    # import in function, because proxystore is not importable in base parsl
    # installation.
    from proxystore.proxy import Proxy
    from proxystore.store import Store, register_store
    from proxystore.connectors.file import FileConnector

    store = Store(name='parsl_store', connector=FileConnector(store_dir="/tmp"))
    register_store(store)

    s = ProxyStoreSerializer(store=store, should_proxy=policy_example)

    # check roundtrip for an int, which will not be proxystored
    roundtripped_7 = s.deserialize(s.serialize(7))
    assert roundtripped_7 == 7
    assert not isinstance(roundtripped_7, Proxy)


    l = [1,2,3]
    k = s.serialize(l)
    roundtripped_l = s.deserialize(s.serialize(l))
    assert roundtripped_l == l
    assert isinstance(roundtripped_l, Proxy)
