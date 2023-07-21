import logging
import pytest
import uuid

import parsl
from parsl.serialize.facade import additional_methods_for_deserialization, methods_for_data, register_method_for_data
from parsl.tests.configs.htex_local import fresh_config


logger = logging.getLogger(__name__)


def local_setup():
    from parsl.serialize.proxystore import ProxyStoreSerializer
    from proxystore.store import Store, register_store
    from proxystore.connectors.file import FileConnector

    parsl.load(fresh_config())

    store = Store(name='parsl_store_' + str(uuid.uuid4()), connector=FileConnector(store_dir="/tmp"))
    register_store(store)

    s = ProxyStoreSerializer(store=store, should_proxy=policy_example)

    global previous_methods
    previous_methods = methods_for_data.copy()

    # get rid of all data serialization methods, in preparation for using only
    # proxystore. put all the old methods as additional methods used only for
    # deserialization, because those will be needed to deserialize the results,
    # which will be serialized using the default serializer set.
    additional_methods_for_deserialization.update(previous_methods)
    methods_for_data.clear()

    register_method_for_data(s)
    logger.info(f"BENC: methods for data: {methods_for_data}")


def local_teardown():
    parsl.dfk().cleanup()
    parsl.clear()

    methods_for_data.clear()
    methods_for_data.update(previous_methods)

    additional_methods_for_deserialization.clear()


@parsl.python_app
def identity(o):
    return o


@parsl.python_app
def is_proxy(o) -> bool:
    from proxystore.proxy import Proxy
    return isinstance(o, Proxy)


def policy_example(o):
    """Example policy will proxy only lists."""
    return isinstance(o, frozenset)


@pytest.mark.local
def test_proxystore_via_apps():
    from proxystore.proxy import Proxy

    # check roundtrip for an int, which should not be proxystored according
    # to example_policy()
    roundtripped_7 = identity(7).result()
    assert roundtripped_7 == 7
    assert not isinstance(roundtripped_7, Proxy)

    # check roundtrip for a list, which should be proxystored according
    # to example_policy()
    v = frozenset([1, 2, 3])

    assert is_proxy(v).result()

    roundtripped_v = identity(v).result()
    assert roundtripped_v == v
