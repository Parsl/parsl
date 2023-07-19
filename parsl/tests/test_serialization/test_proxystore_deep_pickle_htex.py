# test the serializer plugin API
import logging
import pytest
import parsl

from parsl.tests.configs.htex_local import fresh_config as local_config

from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import serialize, deserialize, register_method_for_data, unregister_serializer, clear_serializers

from parsl.serialize.plugin_proxystore_deep_pickle import create_deep_proxystore_serializer

logger = logging.getLogger(__name__)


@parsl.python_app
def func(x, y):
    return x + y + 1


@parsl.python_app
def func2(v):
    return str(v)


class MyDemo:
    def __init__(self, v):
        self._v = v

    def __str__(self):
        return str(self._v)


# TODO: this kind of test, I'd like to be able to run the entire test suite with the
# proxystore serializer configured, so as to see it tested against many cases.
# A bodge is to do that in fresh_config - but that doesn't work in the case of several
# DFKs in a single process, but there is nothing to unconfigure the process-wide state.
# Is this only a test limit? or is it something that should be properly exposed to
# users?

@pytest.mark.local
def test_proxystore_single_call():
    later_c = [v for v in parsl.serialize.facade.methods_for_code]
    later_d = [v for v in parsl.serialize.facade.methods_for_data]
    clear_serializers()
    s = create_deep_proxystore_serializer(policy=MyDemo)

    register_method_for_data(s)

    try:
        assert func(100, 4).result() == 105  # but how do we know this went through proxystore?

        m = MyDemo([1, 2, 3, 4])

        assert func2(m).result() == "[1, 2, 3, 4]"

    finally:
        unregister_serializer(s)

    parsl.serialize.facade.methods_for_code = later_c
    parsl.serialize.facade.methods_for_data = later_d

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
