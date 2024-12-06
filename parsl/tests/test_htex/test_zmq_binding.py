import logging
import pathlib
from typing import Optional
from unittest import mock

import psutil
import pytest
import zmq

from parsl import curvezmq
from parsl.executors.high_throughput.interchange import Interchange
from parsl.executors.high_throughput.manager_selector import RandomManagerSelector


def make_interchange(*, interchange_address: Optional[str], cert_dir: Optional[str]) -> Interchange:
    return Interchange(interchange_address=interchange_address,
                       cert_dir=cert_dir,
                       client_address="127.0.0.1",
                       client_ports=(50055, 50056, 50057),
                       worker_ports=None,
                       worker_port_range=(54000, 55000),
                       hub_address=None,
                       hub_zmq_port=None,
                       heartbeat_threshold=60,
                       logdir=".",
                       logging_level=logging.INFO,
                       manager_selector=RandomManagerSelector(),
                       poll_period=10,
                       run_id="test_run_id")


@pytest.fixture
def encrypted(request: pytest.FixtureRequest):
    if hasattr(request, "param"):
        return request.param
    return True


@pytest.fixture
def cert_dir(encrypted: bool, tmpd_cwd: pathlib.Path):
    if not encrypted:
        return None
    return curvezmq.create_certificates(tmpd_cwd)


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
@mock.patch.object(curvezmq.ServerContext, "socket", return_value=mock.MagicMock())
def test_interchange_curvezmq_sockets(
    mock_socket: mock.MagicMock, cert_dir: Optional[str], encrypted: bool
):
    address = "127.0.0.1"
    ix = make_interchange(interchange_address=address, cert_dir=cert_dir)
    assert isinstance(ix.zmq_context, curvezmq.ServerContext)
    assert ix.zmq_context.encrypted is encrypted
    assert mock_socket.call_count == 5


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_interchange_binding_no_address(cert_dir: Optional[str]):
    ix = make_interchange(interchange_address=None, cert_dir=cert_dir)
    assert ix.interchange_address == "*"


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_interchange_binding_with_address(cert_dir: Optional[str]):
    # Using loopback address
    address = "127.0.0.1"
    ix = make_interchange(interchange_address=address, cert_dir=cert_dir)
    assert ix.interchange_address == address


@pytest.mark.skip("This behaviour is possibly unexpected. See issue #3037")
@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_interchange_binding_with_non_ipv4_address(cert_dir: Optional[str]):
    # Confirm that a ipv4 address is required
    address = "localhost"
    with pytest.raises(zmq.error.ZMQError):
        make_interchange(interchange_address=address, cert_dir=cert_dir)


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_interchange_binding_bad_address(cert_dir: Optional[str]):
    """Confirm that we raise a ZMQError when a bad address is supplied"""
    address = "550.0.0.0"
    with pytest.raises(ValueError):
        make_interchange(interchange_address=address, cert_dir=cert_dir)


@pytest.mark.local
@pytest.mark.parametrize("encrypted", (True, False), indirect=True)
def test_limited_interface_binding(cert_dir: Optional[str]):
    """When address is specified the worker_port would be bound to it rather than to 0.0.0.0"""
    address = "127.0.0.1"
    ix = make_interchange(interchange_address=address, cert_dir=cert_dir)
    ix.worker_result_port
    proc = psutil.Process()
    conns = proc.connections(kind="tcp")

    matched_conns = [conn for conn in conns if conn.laddr.port == ix.worker_result_port]
    assert len(matched_conns) == 1
    # laddr.ip can return ::ffff:127.0.0.1 when using IPv6
    assert address in matched_conns[0].laddr.ip
