# test the serializer plugin API
import logging
import pytest
import parsl

from parsl.tests.configs.htex_local import fresh_config as local_config

from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import serialize, deserialize, register_serializer, unregister_serializer

from parsl.serialize.plugin_proxystore import create_proxystore_serializer

logger = logging.getLogger(__name__)


@parsl.python_app
def func(x, y):
    return x + y + 1


# TODO: this kind of test, I'd like to be able to run the entire test suite with the
# proxystore serializer configured, so as to see it tested against many cases.
# A bodge is to do that in fresh_config - but that doesn't work in the case of several
# DFKs in a single process, but there is nothing to unconfigure the process-wide state.
# Is this only a test limit? or is it something that should be properly exposed to
# users?

@pytest.mark.local
def test_proxystore_single_call():
    s = create_proxystore_serializer()  # TODO this isnt' testing the register_proxystore_serializer function...
    register_serializer(s)

    try:
        assert func(100, 4).result() == 105  # but how do we know this went through proxystore?

    finally:
        unregister_serializer(s)

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
