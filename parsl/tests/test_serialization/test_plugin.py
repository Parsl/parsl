# test the serializer plugin API

import pytest

from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import serialize, deserialize, register_method_for_data, unregister_serializer

B_MAGIC = b'3626874628368432'  # arbitrary const bytestring
V_MAGIC = 777  # arbitrary const object


class ConstSerializer(SerializerBase):
    """This is a test deliberately misbehaving serializer.
    It serializes all values to the same constant, and likewise for
    deserialization.
    """

    # TODO: should be enforcing/defaulting this to class name so that by default we can dynamically load the serializer remotely?
    _identifier = b'parsl.tests.test_serializer.test_plugin ConstSerializer'
    # note a space in the name here not final dot, to distinguish modules vs attribute in module

    # TODO: better enforcement of the presence of these values? tied into abstract base class?

    def serialize(self, o):
        return B_MAGIC

    def deserialize(self, b):
        assert b == B_MAGIC
        return V_MAGIC

# ugh... registering in globus state which will screw with other tests
# so should protect with a finally block:
# serializer registration is, generally, intended to be a not-undoable
# operation, though...


@pytest.mark.local
def test_const_inprocess():
    s = ConstSerializer()
    register_method_for_data(s)

    try:

        # check that we aren't using one of the real serializers
        # that really works
        assert deserialize(serialize(1)) != 1

        # and that the behaviour looks like ConstSerializer
        assert serialize(1) == s.identifier + b'\n' + B_MAGIC
        assert deserialize(serialize(1)) == V_MAGIC
    finally:
        unregister_serializer(s)

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
