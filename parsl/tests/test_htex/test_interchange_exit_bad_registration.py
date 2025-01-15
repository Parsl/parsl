import json
import logging
import os
import pickle
import platform
import subprocess
import time

import psutil
import pytest
import zmq

import parsl.executors.high_throughput.zmq_pipes as zmq_pipes
from parsl.executors.high_throughput.executor import DEFAULT_INTERCHANGE_LAUNCH_CMD
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector
from parsl.version import VERSION as PARSL_VERSION

P_ms = 10


@pytest.mark.local
def test_exit_with_bad_registration(tmpd_cwd, try_assert):
    """Test that the interchange exits when it receives a bad registration message.
    This complements parsl/tests/test_scaling/test_worker_interchange_bad_messages_3262.py
    which tests that the interchange is resistent to other forms of bad message.
    """

    outgoing_q = zmq_pipes.TasksOutgoing(
        "127.0.0.1", (49152, 65535), None
    )
    incoming_q = zmq_pipes.ResultsIncoming(
        "127.0.0.1", (49152, 65535), None
    )
    command_client = zmq_pipes.CommandClient(
        "127.0.0.1", (49152, 65535), None
    )

    interchange_config = {"client_address": "127.0.0.1",
                          "client_ports": (outgoing_q.port,
                                           incoming_q.port,
                                           command_client.port),
                          "interchange_address": "127.0.0.1",
                          "worker_ports": None,
                          "worker_port_range": (50000, 60000),
                          "hub_address": None,
                          "hub_zmq_port": None,
                          "logdir": tmpd_cwd,
                          "heartbeat_threshold": 120,
                          "poll_period": P_ms,
                          "logging_level": logging.DEBUG,
                          "cert_dir": None,
                          "manager_selector": RandomManagerSelector(),
                          "run_id": "test"
                          }

    config_pickle = pickle.dumps(interchange_config)

    interchange_proc = subprocess.Popen(DEFAULT_INTERCHANGE_LAUNCH_CMD, stdin=subprocess.PIPE)
    stdin = interchange_proc.stdin
    assert stdin is not None, "Popen should have created an IO object (vs default None) because of PIPE mode"

    stdin.write(config_pickle)
    stdin.flush()
    stdin.close()

    # wait for interchange to be alive, by waiting for the command thread to become
    # responsive. if the interchange process didn't start enough to get the command
    # thread running, this will time out.

    (task_port, result_port) = command_client.run("WORKER_PORTS", timeout_s=120)

    # now we'll assume that if the interchange command thread is responding,
    # then the worker polling code is also running and that the interchange has
    # started successfully.

    # send bad registration message as if from a new worker pool. The badness here
    # is that the Python version does not match the real Python version - which
    # unlike some other bad interchange messages, should cause the interchange
    # to shut down.

    msg = {'type': 'registration',
           'parsl_v': PARSL_VERSION,
           'python_v': "{}.{}.{}".format(1, 1, 1),  # this is the bad bit
           'worker_count': 1,
           'uid': 'testuid',
           'block_id': 0,
           'start_time': time.time(),
           'prefetch_capacity': 0,
           'max_capacity': 1,
           'os': platform.system(),
           'hostname': platform.node(),
           'dir': os.getcwd(),
           'cpu_count': psutil.cpu_count(logical=False),
           'total_memory': psutil.virtual_memory().total,
           }

    # connect to worker port and send this message.

    context = zmq.Context()
    channel_timeout = 10000  # in milliseconds
    task_channel = context.socket(zmq.DEALER)
    task_channel.setsockopt(zmq.LINGER, 0)
    task_channel.setsockopt(zmq.IDENTITY, b'testid')

    task_channel.set_hwm(0)
    task_channel.setsockopt(zmq.SNDTIMEO, channel_timeout)
    task_channel.connect(f"tcp://127.0.0.1:{task_port}")

    b_msg = json.dumps(msg).encode('utf-8')

    task_channel.send(b_msg)

    # check that the interchange exits within some reasonable time
    try_assert(lambda: interchange_proc.poll() is not None, "Interchange did not exit after killing watched client process", timeout_ms=5000)

    # See issue #3697 - ideally the interchange would exit cleanly, but it does not.
    # assert interchange_proc.poll() == 0, "Interchange exited with an error code, not 0"

    task_channel.close()
    context.term()
