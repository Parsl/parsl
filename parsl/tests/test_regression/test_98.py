'''
Regression test for issue #98
'''
import argparse

import pytest

import parsl
from parsl.dataflow.dflow import DataFlowKernel
from parsl.tests.configs.local_threads import config


@pytest.mark.local
def test_immutable_config(n=2):
    """Regression test for immutable config #98
    """

    original = str(config)
    dfk = DataFlowKernel(config=config)
    after = str(config)

    dfk.cleanup()
    assert original == after, "Config modified"
