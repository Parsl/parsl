import pytest
from parsl.serialize import serialize, deserialize
from parsl.serialize.concretes import DillSerializer, PickleSerializer


@pytest.mark.local
def test_serialize():
    assert deserialize(serialize(1)) == 1


@pytest.mark.local
def test_pickle_wrapper():
    s = PickleSerializer()
    d = s.serialize(1)
    assert isinstance(d, bytes)
    assert s.deserialize(d) == 1


@pytest.mark.local
def test_dill_wrapper():
    s = DillSerializer()
    d = s.serialize(1)
    assert isinstance(d, bytes)
    assert s.deserialize(d) == 1
