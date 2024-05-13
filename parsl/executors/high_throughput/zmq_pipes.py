#!/usr/bin/env python3

import logging
import threading
import time
from typing import Optional

import zmq

from parsl import curvezmq
from parsl.addresses import tcp_url
from parsl.errors import InternalConsistencyError
from parsl.executors.high_throughput.errors import (
    CommandClientBadError,
    CommandClientTimeoutError,
)

logger = logging.getLogger(__name__)


class CommandClient:
    """ CommandClient
    """
    def __init__(self, ip_address, port_range, cert_dir: Optional[str] = None):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)

        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        cert_dir: str | None
            Path to the certificate directory. Setting this to None will disable encryption.
            default: None

        """
        self.zmq_context = curvezmq.ClientContext(cert_dir)
        self.ip_address = ip_address
        self.port_range = port_range
        self.port = None
        self.create_socket_and_bind()
        self._lock = threading.Lock()
        self.ok = True

    def create_socket_and_bind(self):
        """ Creates socket and binds to a port.

        Upon recreating the socket, we bind to the same port.
        """
        self.zmq_socket = self.zmq_context.socket(zmq.REQ)
        self.zmq_socket.setsockopt(zmq.LINGER, 0)
        if self.port is None:
            self.port = self.zmq_socket.bind_to_random_port(tcp_url(self.ip_address),
                                                            min_port=self.port_range[0],
                                                            max_port=self.port_range[1])
        else:
            self.zmq_socket.bind(tcp_url(self.ip_address, self.port))

    def run(self, message, max_retries=3, timeout_s=None):
        """ This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """
        if not self.ok:
            raise CommandClientBadError()

        start_time_s = time.monotonic()

        reply = '__PARSL_ZMQ_PIPES_MAGIC__'
        with self._lock:
            for _ in range(max_retries):
                try:
                    logger.debug("Sending command client command")

                    if timeout_s is not None:
                        remaining_time_s = start_time_s + timeout_s - time.monotonic()
                        poll_result = self.zmq_socket.poll(timeout=remaining_time_s * 1000, flags=zmq.POLLOUT)
                        if poll_result == zmq.POLLOUT:
                            pass  # this is OK, so continue
                        elif poll_result == 0:
                            raise CommandClientTimeoutError("Waiting for command channel to be ready for a command")
                        else:
                            raise InternalConsistencyError(f"ZMQ poll returned unexpected value: {poll_result}")

                    self.zmq_socket.send_pyobj(message, copy=True)

                    if timeout_s is not None:
                        logger.debug("Polling for command client response or timeout")
                        remaining_time_s = start_time_s + timeout_s - time.monotonic()
                        poll_result = self.zmq_socket.poll(timeout=remaining_time_s * 1000, flags=zmq.POLLIN)
                        if poll_result == zmq.POLLIN:
                            pass  # this is OK, so continue
                        elif poll_result == 0:
                            logger.error("Command timed-out - command client is now bad forever")
                            self.ok = False
                            raise CommandClientTimeoutError("Waiting for a reply from command channel")
                        else:
                            raise InternalConsistencyError(f"ZMQ poll returned unexpected value: {poll_result}")

                    logger.debug("Receiving command client response")
                    reply = self.zmq_socket.recv_pyobj()
                    logger.debug("Received command client response")
                except zmq.ZMQError:
                    logger.exception("Potential ZMQ REQ-REP deadlock caught")
                    logger.info("Trying to reestablish context")
                    self.zmq_context.recreate()
                    self.create_socket_and_bind()
                else:
                    break

        if reply == '__PARSL_ZMQ_PIPES_MAGIC__':
            logger.error("Command channel run retries exhausted. Unable to run command")
            raise Exception("Command Channel retries exhausted")

        return reply

    def close(self):
        self.zmq_socket.close()
        self.zmq_context.term()


class TasksOutgoing:
    """ Outgoing task queue from the executor to the Interchange
    """
    def __init__(self, ip_address, port_range, cert_dir: Optional[str] = None):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)

        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        cert_dir: str | None
            Path to the certificate directory. Setting this to None will disable encryption.
            default: None

        """
        self.zmq_context = curvezmq.ClientContext(cert_dir)
        self.zmq_socket = self.zmq_context.socket(zmq.DEALER)
        self.zmq_socket.set_hwm(0)
        self.port = self.zmq_socket.bind_to_random_port(tcp_url(ip_address),
                                                        min_port=port_range[0],
                                                        max_port=port_range[1])
        self.poller = zmq.Poller()
        self.poller.register(self.zmq_socket, zmq.POLLOUT)

    def put(self, message):
        """ This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """
        timeout_ms = 1
        while True:
            socks = dict(self.poller.poll(timeout=timeout_ms))
            if self.zmq_socket in socks and socks[self.zmq_socket] == zmq.POLLOUT:
                # The copy option adds latency but reduces the risk of ZMQ overflow
                logger.debug("Sending TasksOutgoing message")
                self.zmq_socket.send_pyobj(message, copy=True)
                logger.debug("Sent TasksOutgoing message")
                return
            else:
                timeout_ms *= 2
                logger.debug("Not sending due to non-ready zmq pipe, timeout: {} ms".format(timeout_ms))

    def close(self):
        self.zmq_socket.close()
        self.zmq_context.term()


class ResultsIncoming:
    """ Incoming results queue from the Interchange to the executor
    """

    def __init__(self, ip_address, port_range, cert_dir: Optional[str] = None):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)

        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        cert_dir: str | None
            Path to the certificate directory. Setting this to None will disable encryption.
            default: None

        """
        self.zmq_context = curvezmq.ClientContext(cert_dir)
        self.results_receiver = self.zmq_context.socket(zmq.DEALER)
        self.results_receiver.set_hwm(0)
        self.port = self.results_receiver.bind_to_random_port(tcp_url(ip_address),
                                                              min_port=port_range[0],
                                                              max_port=port_range[1])
        self.poller = zmq.Poller()
        self.poller.register(self.results_receiver, zmq.POLLIN)

    def get(self, timeout_ms=None):
        """Get a message from the queue, returning None if timeout expires
        without a message. timeout is measured in milliseconds.
        """
        socks = dict(self.poller.poll(timeout=timeout_ms))
        if self.results_receiver in socks and socks[self.results_receiver] == zmq.POLLIN:
            m = self.results_receiver.recv_multipart()
            logger.debug("Received ResultsIncoming message")
            return m
        else:
            return None

    def close(self):
        self.results_receiver.close()
        self.zmq_context.term()
