# test the serializer plugin API
import logging

import pytest

import parsl
from parsl.serialize.base import SerializerBase
from parsl.serialize.facade import (
    deserialize,
    register_method_for_data,
    serialize,
    unregister_serializer,
)
from parsl.serialize.plugin_serpent import SerpentSerializer
from parsl.tests.configs.htex_local import fresh_config as local_config

logger = logging.getLogger(__name__)


@parsl.python_app
def func(x):
    return x + 1


@pytest.mark.local
def test_serpent_single_call():
    s = SerpentSerializer()
    register_method_for_data(s)

    try:
        assert func(100).result() == 101  # but how do we know this went through serpent? TODO

    finally:
        unregister_serializer(s)

    assert deserialize(serialize(1)) == 1  # check serialisation is basically working again
