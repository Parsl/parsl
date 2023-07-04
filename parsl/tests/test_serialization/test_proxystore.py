import pytest
from parsl.serialize.plugin_proxystore import create_proxystore_serializer


@pytest.mark.local
def test_proxystore_wrapper():
    s = create_proxystore_serializer()
    d = s.serialize(1)
    assert isinstance(d, bytes)
    assert s.deserialize(d) == 1
