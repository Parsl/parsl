import pytest
from parsl.serialize.concretes import ProxyStoreSerializer


@pytest.mark.local
def test_proxystore_wrapper():
    s = ProxyStoreSerializer()
    d = s.serialize(1)
    assert isinstance(d, bytes)
    assert s.deserialize(d) == 1
