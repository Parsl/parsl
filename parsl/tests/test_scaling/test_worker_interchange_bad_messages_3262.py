import os
import signal
import time

import pytest
import zmq

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

T_s = 1


def fresh_config():
    htex = HighThroughputExecutor(
               heartbeat_period=1 * T_s,
               heartbeat_threshold=3 * T_s,
               label="htex_local",
               worker_debug=True,
               cores_per_worker=1,
               encrypted=False,
               provider=LocalProvider(
                   init_blocks=0,
                   min_blocks=0,
                   max_blocks=0,
                   launcher=SimpleLauncher(),
               ),
           )
    c = Config(
        executors=[htex],
        strategy='none',
        strategy_period=0.5,
    )
    return c, htex


@parsl.python_app
def app():
    return 7


@pytest.mark.local
@pytest.mark.parametrize("msg",
                         (b'FuzzyByte\rSTREAM',  # not JSON
                          b'{}',  # missing fields
                          b'{"type":"heartbeat"}',  # regression test #3262
                          )
                         )
def test_bad_messages(try_assert, msg):
    """This tests that the interchange is resilient to a few different bad
    messages: malformed messages caused by implementation errors, and
    heartbeat messages from managers that are not registered.

    The heartbeat test is a regression test for issues #3262, #3632
    """

    c, htex = fresh_config()

    with parsl.load(c):

        # send a bad message into the interchange on the task_outgoing worker
        # channel, and then check that the interchange is still alive enough
        # that we can scale out a block and run a task.

        (task_port, result_port) = htex.command_client.run("WORKER_PORTS")

        context = zmq.Context()
        channel_timeout = 10000  # in milliseconds
        task_channel = context.socket(zmq.DEALER)
        task_channel.setsockopt(zmq.LINGER, 0)
        task_channel.setsockopt(zmq.IDENTITY, b'testid')

        task_channel.set_hwm(0)
        task_channel.setsockopt(zmq.SNDTIMEO, channel_timeout)
        task_channel.connect(f"tcp://localhost:{task_port}")

        task_channel.send(msg)

        # If the interchange exits, it's likely that this test will hang rather
        # than raise an error, because the interchange interaction code
        # assumes the interchange is always there.
        # In the case of issue #3262, an exception message goes to stderr, and
        # no error goes to the interchange log file.
        htex.scale_out_facade(1)
        try_assert(lambda: len(htex.connected_managers()) == 1, timeout_ms=10000)

        assert app().result() == 7
