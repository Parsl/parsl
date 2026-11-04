import logging
import pickle
import threading
import time
import uuid
from typing import cast
from unittest import mock

import msgspec
import pytest
import zmq

from parsl import curvezmq
from parsl.executors.high_throughput import protocol as proto
from parsl.executors.high_throughput.errors import VersionMismatch
from parsl.executors.high_throughput.interchange import Interchange
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector
from parsl.serialize import deserialize
from parsl.version import VERSION as PARSL_VERSION


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


def send_from_manager(ix: Interchange, manager_id: bytes, msg: proto.Message) -> None:
    send_bytes_from_manager(ix, manager_id, msg.encode())


def registration_msg(ix: Interchange, **kw) -> proto.Registration:
    return proto.Registration(**{
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
        'drain_time': float('inf'),
        **kw})


def shutdown_sent(ix: Interchange, manager_id: bytes) -> bool:
    mock_sock = cast(mock.MagicMock, ix.manager_sock)
    call_args = [manager_id, proto.Shutdown().encode()]
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

    send_from_manager(ix, manager_id, proto.Result(task_id=1, result=b''))
    ix.expire_drained_managers(set(), None)

    assert shutdown_sent(ix, manager_id)
    assert manager_id not in ix._ready_managers


@pytest.mark.local
def test_version_mismatch_reported_despite_missing_fields(ix, manager_id):
    raw = {'type': 'registration', 'parsl_v': "2016.01.01", 'python_v': ix.current_platform['python_v']}
    ix.socks = {ix.manager_sock: zmq.POLLIN}
    cast(mock.MagicMock, ix.manager_sock).recv_multipart.return_value = [manager_id, pickle.dumps(raw)]
    kill_event = threading.Event()
    ix.process_manager_socket_message(set(), None, kill_event)

    assert kill_event.is_set()
    assert manager_id not in ix._ready_managers
    (sent,), _ = cast(mock.MagicMock, ix.results_outgoing).send.call_args
    failure = proto.decode(sent, proto.INTERCHANGE_TO_EXECUTOR)
    assert isinstance(failure, proto.InterchangeFailure)
    assert isinstance(deserialize(failure.exception), VersionMismatch)


@pytest.mark.local
def test_legacy_manager_without_drain_time(ix, manager_id):
    # ensure new/post-drain_time interchanges work with old/pre-drain_time managers
    reg_msg = {'type': 'registration', **msgspec.structs.asdict(registration_msg(ix))}
    del reg_msg['drain_time']
    send_bytes_from_manager(ix, manager_id, pickle.dumps(reg_msg))
    assert manager_id in ix._ready_managers, "manager should have been registered"

    ix.expire_drained_managers(set(), None)
    assert not shutdown_sent(ix, manager_id), "ix should wait for legacy manager to request shutdown"

    send_from_manager(ix, manager_id, proto.Drain())
    ix.expire_drained_managers(set(), None)

    assert shutdown_sent(ix, manager_id)
    assert manager_id not in ix._ready_managers
