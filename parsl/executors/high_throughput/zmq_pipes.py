#!/usr/bin/env python3

import zmq
import logging
import threading

logger = logging.getLogger(__name__)


class CommandClient:
    """ CommandClient
    """
    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """
        self.context = zmq.Context()
        self.ip_address = ip_address
        self.port_range = port_range
        self.port = None
        self.create_socket_and_bind()
        self._lock = threading.Lock()

    def create_socket_and_bind(self):
        """ Creates socket and binds to a port.

        Upon recreating the socket, we bind to the same port.
        """
        self.zmq_socket = self.context.socket(zmq.REQ)
        self.zmq_socket.setsockopt(zmq.LINGER, 0)
        if self.port is None:
            self.port = self.zmq_socket.bind_to_random_port("tcp://{}".format(self.ip_address),
                                                            min_port=self.port_range[0],
                                                            max_port=self.port_range[1])
        else:
            self.zmq_socket.bind("tcp://{}:{}".format(self.ip_address, self.port))

    def run(self, message, max_retries=3):
        """ This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """
        reply = '__PARSL_ZMQ_PIPES_MAGIC__'
        with self._lock:
            for i in range(max_retries):
                try:
                    self.zmq_socket.send_pyobj(message, copy=True)
                    reply = self.zmq_socket.recv_pyobj()
                except zmq.ZMQError:
                    logger.exception("Potential ZMQ REQ-REP deadlock caught")
                    logger.info("Trying to reestablish context")
                    self.zmq_socket.close()
                    self.context.destroy()
                    self.context = zmq.Context()
                    self.create_socket_and_bind()
                else:
                    break

        if reply == '__PARSL_ZMQ_PIPES_MAGIC__':
            logger.error("Command channel run retries exhausted. Unable to run command")
            raise Exception("Command Channel retries exhausted")

        return reply

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class TasksOutgoing:
    """ Outgoing task queue from the executor to the Interchange
    """
    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.DEALER)
        self.zmq_socket.set_hwm(0)
        self.port = self.zmq_socket.bind_to_random_port("tcp://{}".format(ip_address),
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
                self.zmq_socket.send_pyobj(message, copy=True)
                return
            else:
                timeout_ms *= 2
                logger.debug("Not sending due to non-ready zmq pipe, timeout: {} ms".format(timeout_ms))

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class ResultsIncoming:
    """ Incoming results queue from the Interchange to the executor
    """

    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """
        self.context = zmq.Context()
        self.results_receiver = self.context.socket(zmq.DEALER)
        self.results_receiver.set_hwm(0)
        self.port = self.results_receiver.bind_to_random_port("tcp://{}".format(ip_address),
                                                              min_port=port_range[0],
                                                              max_port=port_range[1])

    def get(self):
        return self.results_receiver.recv_multipart()

    def close(self):
        self.results_receiver.close()
        self.context.term()
