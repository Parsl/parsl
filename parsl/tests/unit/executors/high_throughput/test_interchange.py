import logging
import pickle
import threading
import time
import uuid
from typing import TypeVar, cast
from unittest import mock

import msgspec
import pytest
import zmq

from parsl import curvezmq
from parsl.executors.high_throughput import protocol
from parsl.executors.high_throughput.interchange import Interchange
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector
from parsl.version import VERSION as PARSL_VERSION

R = TypeVar("R", bound=protocol.Message)


@pytest.fixture
def ix(tmpd_cwd):
    with mock.patch.object(curvezmq.ServerContext, "socket", side_effect=lambda *a, **kw: mock.MagicMock()):
        yield Interchange(interchange_address="127.0.0.1",
                          cert_dir=None,
                          client_address="127.0.0.1",
                          client_ports=(50055, 50056, 50057),
                          worker_port=None,
                          worker_port_range=(54000, 55000),
                          hub_address=None,
                          hub_zmq_port=None,
                          heartbeat_threshold=60,
                          logdir=str(tmpd_cwd),
                          logging_level=logging.INFO,
                          log_config=None,
                          manager_selector=RandomManagerSelector(),
                          poll_period=10,
                          run_id="test_run_id",
                          _check_python_mismatch=True)


@pytest.fixture
def manager_id() -> bytes:
    return f"manager-{uuid.uuid4()}".encode()


def send_bytes_from_manager(ix: Interchange, manager_id: bytes, data: bytes) -> None:
    ix.socks = {ix.manager_sock: zmq.POLLIN}
    mock_sock = cast(mock.MagicMock, ix.manager_sock)
    mock_sock.recv_multipart.return_value = [manager_id, data]
    ix.process_manager_socket_message(set(), None, threading.Event())


def send_from_manager(ix: Interchange, manager_id: bytes, msg: protocol.Message) -> None:
    send_bytes_from_manager(ix, manager_id, msg.to_wire())


def send_command_bytes(ix: Interchange, data: bytes) -> bytes:
    ix.socks = {ix.command_channel: zmq.POLLIN}
    mock_sock = cast(mock.MagicMock, ix.command_channel)
    mock_sock.recv.return_value = data
    mock_sock.send.reset_mock()
    ix.process_command(None)
    (reply,), _ = mock_sock.send.call_args
    return reply


def send_command(ix: Interchange, command: protocol.Command[R]) -> R:
    return protocol.from_wire(send_command_bytes(ix, command.to_wire()), command.reply_channel)


def registration_msg(ix: Interchange, **kw) -> protocol.Registration:
    return protocol.Registration(**{
        'parsl_v': PARSL_VERSION,
        'python_v': ix.current_platform['python_v'],
        'worker_count': 1,
        'uid': 'testuid',
        'block_id': '0',
        'start_time': time.time(),
        'prefetch_capacity': 0,
        'max_capacity': 1,
        'packages': {},
        'os': 'testos',
        'hostname': 'testhost',
        'dir': '/test/dir',
        'cpu_count': 1,
        'total_memory': 1,
        **kw})


def shutdown_sent(ix: Interchange, manager_id: bytes) -> bool:
    mock_sock = cast(mock.MagicMock, ix.manager_sock)
    call_args = [manager_id, protocol.Shutdown().to_wire()]
    return mock.call(call_args) in mock_sock.send_multipart.call_args_list


@pytest.mark.local
def test_no_drain_before_drain_time(ix, manager_id):
    reg_msg = registration_msg(ix, drain_time=time.time() + 3600)
    send_from_manager(ix, manager_id, reg_msg)
    assert manager_id in ix._ready_managers, "manager should have been registered"

    ix.expire_drained_managers(set(), None)

    assert not ix._ready_managers[manager_id]['draining']
    assert not shutdown_sent(ix, manager_id)


@pytest.mark.local
@pytest.mark.parametrize("is_interesting", (True, False))
def test_idle_manager_shut_down_at_drain_time(ix, manager_id, is_interesting):
    reg_msg = registration_msg(ix, drain_time=time.time())
    send_from_manager(ix, manager_id, reg_msg)
    assert manager_id in ix._ready_managers, "manager should have been registered"

    interesting_managers = set([manager_id] if is_interesting else [])
    ix.expire_drained_managers(interesting_managers, None)

    assert shutdown_sent(ix, manager_id)
    assert manager_id not in ix._ready_managers
    assert manager_id not in interesting_managers


@pytest.mark.local
def test_draining_manager_shut_down_after_last_result(ix, manager_id):
    reg_msg = registration_msg(ix, drain_time=time.time())
    send_from_manager(ix, manager_id, reg_msg)
    assert manager_id in ix._ready_managers, "manager should have been registered"

    ix._ready_managers[manager_id]['tasks'].append(1)
    ix.expire_drained_managers(set(), None)

    assert ix._ready_managers[manager_id]['draining']
    assert not shutdown_sent(ix, manager_id)

    send_from_manager(ix, manager_id, protocol.Result(task_id=1, result=b''))
    ix.expire_drained_managers(set(), None)

    assert shutdown_sent(ix, manager_id)
    assert manager_id not in ix._ready_managers


@pytest.mark.local
def test_legacy_manager_without_drain_time(ix, manager_id):
    # ensure new/post-drain_time interchanges work with old/pre-drain_time managers
    reg_msg = {'type': 'registration', **msgspec.structs.asdict(registration_msg(ix))}
    del reg_msg['drain_time']
    send_bytes_from_manager(ix, manager_id, pickle.dumps(reg_msg))
    assert manager_id in ix._ready_managers, "manager should have been registered"

    ix.expire_drained_managers(set(), None)
    assert not shutdown_sent(ix, manager_id), "ix should wait for legacy manager to request shutdown"

    send_from_manager(ix, manager_id, protocol.Drain())
    ix.expire_drained_managers(set(), None)

    assert shutdown_sent(ix, manager_id)
    assert manager_id not in ix._ready_managers


@pytest.mark.local
def test_commands(ix, manager_id):
    ix.worker_port = 54321
    send_from_manager(ix, manager_id, registration_msg(ix, block_id='7'))

    assert send_command(ix, protocol.WorkerBinds()) == protocol.WorkerBindsReply(port=54321)
    assert send_command(ix, protocol.ConnectedBlocks()) == protocol.ConnectedBlocksReply(block_ids=['7'])
    managers = send_command(ix, protocol.Managers()).managers
    assert [m['manager'] for m in managers] == [manager_id.decode()]
    assert managers[0]['active']
    assert send_command(ix, protocol.ManagersPackages()) == protocol.ManagersPackagesReply(packages={manager_id.decode(): {}})
    assert send_command(ix, protocol.HoldManager(manager_id=manager_id.decode())) == protocol.HoldManagerReply()
    assert not ix._ready_managers[manager_id]['active']


@pytest.mark.local
@pytest.mark.parametrize("data", (pickle.dumps("MANGERS_PACKAGES"), b"not a pickle"))
def test_unknown_command_answered(ix, data):
    assert send_command_bytes(ix, data) == protocol.CommandError().to_wire() == pickle.dumps(None)
