import logging

import psutil
import pytest
import zmq

from parsl.executors.high_throughput.interchange import Interchange


def test_interchange_binding_no_address():
    ix = Interchange()
    assert ix.interchange_address == "*"


def test_interchange_binding_with_address():
    # Using loopback address
    address = "127.0.0.1"
    ix = Interchange(interchange_address=address)
    assert ix.interchange_address == address


def test_interchange_binding_with_non_ipv4_address():
    # Confirm that a ipv4 address is required
    address = "localhost"
    with pytest.raises(zmq.error.ZMQError):
        Interchange(interchange_address=address)


def test_interchange_binding_bad_address():
    """ Confirm that we raise a ZMQError when a bad address is supplied"""
    address = "550.0.0.0"
    with pytest.raises(zmq.error.ZMQError):
        Interchange(interchange_address=address)


def test_limited_interface_binding():
    """ When address is specified the worker_port would be bound to it rather than to 0.0.0.0"""
    address = "127.0.0.1"
    ix = Interchange(interchange_address=address)
    ix.worker_result_port
    proc = psutil.Process()
    conns = proc.connections(kind="tcp")

    matched_conns = [conn for conn in conns if conn.laddr.port == ix.worker_result_port]
    assert len(matched_conns) == 1
    assert matched_conns[0].laddr.ip == address
