# test the serializer plugin API
import logging
import pytest
import parsl

from parsl.tests.configs.htex_local import fresh_config as local_config

from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import serialize, deserialize, register_method_for_data, unregister_serializer

logger = logging.getLogger(__name__)

B_MAGIC = b'3626874628368432'  # arbitrary const bytestring
V_MAGIC = 777  # arbitrary const object


class XXXXSerializer(SerializerBase):
    """This is a test deserializer but puts some padding round to help distinguish...
    """

    _for_code = True
    _for_data = True

    # TODO: should be enforcing/defaulting this to class name so that by default we can dynamically load the serializer remotely?
    _identifier = b'parsl.tests.test_serialization.test_plugin_htex XXXXSerializer'
    # note a space in the name here not final dot, to distinguish modules vs attribute in module

    # TODO: better enforcement of the presence of these values? tied into abstract base class?

    def serialize(self, o):
        import dill
        logger.error(f"BENC: XXXX serializer serializing value {o} of type {type(o)}")
        return dill.dumps(o)

    def deserialize(self, b):
        import dill
        return dill.loads(b)

# ugh... registering in globus state which will screw with other tests
# so should protect with a finally block:
# serializer registration is, generally, intended to be a not-undoable
# operation, though...


@parsl.python_app
def func(x):
    return x


@pytest.mark.local
def test_const_inprocess():
    s = XXXXSerializer()
    register_method_for_data(s)

    try:
        assert func(100).result() == 100  # but how do we know this went through XXXXSerializer? (or not)

    finally:
        unregister_serializer(s)

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
