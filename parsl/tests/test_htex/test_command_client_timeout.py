import threading
import time

import pytest
import zmq

from parsl import curvezmq
from parsl.executors.high_throughput.errors import (
    CommandClientBadError,
    CommandClientTimeoutError,
)
from parsl.executors.high_throughput.zmq_pipes import CommandClient

# Time constant used for timeout tests: various delays and
# timeouts will be appropriate multiples of this, but the
# value of T itself should not matter too much as long as
# it is big enough for zmq connections to happen successfully.
T = 0.25


@pytest.mark.local
def test_command_not_sent() -> None:
    """Tests timeout on command send.
    """
    # RFC6335 ephemeral port range
    cc = CommandClient("127.0.0.1", (49152, 65535))

    # cc will now wait for a connection, but we won't do anything to make the
    # other side of the connection exist, so any command given to cc should
    # timeout.

    with pytest.raises(CommandClientTimeoutError):
        cc.run("SOMECOMMAND", timeout_s=T)

    cc.close()


@pytest.mark.local
def test_command_ignored() -> None:
    """Tests timeout on command response.
    Tests that we timeout after a response and that the command client
    sets itself into a bad state.

    This only tests sequential access to the command client, even though
    htex makes multithreaded use of the command client: see issue #3376 about
    that lack of thread safety.
    """
    # RFC6335 ephemeral port range
    cc = CommandClient("127.0.0.1", (49152, 65535))

    ic_ctx = curvezmq.ServerContext(None)
    ic_channel = ic_ctx.socket(zmq.REP)
    ic_channel.connect(f"tcp://127.0.0.1:{cc.port}")

    with pytest.raises(CommandClientTimeoutError):
        cc.run("SLOW_COMMAND", timeout_s=T)

    req = ic_channel.recv_pyobj()
    assert req == "SLOW_COMMAND", "Should have received command on interchange side"
    assert not cc.ok, "CommandClient should have set itself to bad"

    with pytest.raises(CommandClientBadError):
        cc.run("ANOTHER_COMMAND")

    cc.close()
    ic_channel.close()
