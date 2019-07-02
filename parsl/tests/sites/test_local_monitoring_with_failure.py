import pytest

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.app.app import python_app
from parsl.tests.configs.local_threads_monitoring import config


@python_app
def python_app_2(f):
    return "R2"

@python_app
def python_app_1():
    # return "r1"
    raise ValueError("deliberate failure")

@pytest.mark.local
def test_python(N=2):

    parsl.set_stream_logger()
    parsl.load(config)

    f1 = python_app_1()
    f2 = python_app_2(f1)

    f2.exception()
    # wait for a result or exception, but do not throw exception if
    # that is what arrives

    parsl.dfk().cleanup()
