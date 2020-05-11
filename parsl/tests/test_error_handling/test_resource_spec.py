import argparse
import time

import pytest

import parsl
from parsl.app.app import python_app, bash_app
from parsl.tests.configs.local_threads import config
from parsl.executors.errors import UnsupportedFeatureError
from parsl.executors import WorkQueueExecutor

local_config = config


@python_app
def double(x, parsl_resource_specification={}):
    return x * 2


def test_resource(n=2):
    spec={'cores': 2, 'memory': '1GiB'}
    fut = double(n, parsl_resource_specification=spec)
    try:
        fut.result()
    except Exception as e:
        assert isinstance(e, UnsupportedFeatureError)
    else:
        executor = list(parsl.dfk().executors.values())[0]
        assert isinstance(executor, WorkQueueExecutor) 


if __name__ == '__main__':

    parsl.load(local_config)
    x = test_resource(2)
