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
        self._my_thread = None

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

        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """

        if self._my_thread is None:
            self._my_thread = threading.current_thread()
        elif self._my_thread != threading.current_thread():
            logger.warning(f"Command socket suspicious thread usage: {self._my_thread} vs {threading.current_thread()}")
        # otherwise, _my_thread and current_thread match, which is ok and no need to log

        reply = '__PARSL_ZMQ_PIPES_MAGIC__'
        logger.debug("acquiring command lock")
        with self._lock:
            logger.debug("acquired command lock")
            for i in range(max_retries):
                logger.debug(f"try {i} for command {message}")
                try:
                    logger.debug("waiting for command client to be ready")
                    r = self.zmq_socket.poll(flags=zmq.PollEvent.POLLOUT, timeout=30000)   # TODO: don't hardcode this timeout
                    if r == 0:  # timeout
                        raise RuntimeError("CommandClient poll-before-command timed out")
                    # TODO: what other values of r are correct?
                    logger.debug("Sending command client command")
                    self.zmq_socket.send_pyobj(message, copy=True)
                    logger.debug(f"waiting for response from command {message}")
                    r = self.zmq_socket.poll(timeout=30000)   # TODO: don't hardcode this timeout
                    if r == 0:  # timeout
                        raise RuntimeError("CommandClient poll-before-result timed out")
                    # TODO: what other values of r are correct?

                    reply = self.zmq_socket.recv_pyobj(flags=zmq.NOBLOCK)
                    # Don't block here: we know there's a message because
                    # of the successful poll... if that happens to be not
                    # true, raise an exception rather than hanging. However
                    # that's a bug in the code, not an expected occurence.

                    logger.debug(f"got response from command {message}")
                except zmq.ZMQError:
                    logger.exception("Potential ZMQ REQ-REP deadlock caught")
                    logger.info("Trying to reestablish context after ZMQError")
                    self.zmq_socket.close()
                    self.context.destroy()
                    self.context = zmq.Context()
                    self.create_socket_and_bind()
                    self._my_thread = None
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
        self._lock = threading.Lock()

    def put(self, message):
        """ This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """
        logger.debug("Putting task to outgoing_q")
        timeout_ms = 1
        with self._lock:
            while True:
                socks = dict(self.poller.poll(timeout=timeout_ms))
                if self.zmq_socket in socks and socks[self.zmq_socket] == zmq.POLLOUT:
                    # The copy option adds latency but reduces the risk of ZMQ overflow
                    logger.debug("Sending TasksOutgoing message")
                    self.zmq_socket.send_pyobj(message, copy=True)
                    logger.debug("Sent TasksOutgoing message")
                    return
                else:
                    timeout_ms = max(timeout_ms, 1)
                    timeout_ms *= 2
                    logger.error("Not sending due to non-ready zmq pipe, timeout: {} ms".format(timeout_ms))
                    if timeout_ms >= 10000:
                        logger.error("Hit big timeout, raising exception")
                        raise RuntimeError("BENC: hit big timeout for pipe put - failing rather than trying forever")

    def close(self):
        with self._lock:
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
        self._lock = threading.Lock()

    def get(self):
        logger.debug("Waiting for ResultsIncoming lock")
        with self._lock:
            logger.debug("Waiting for ResultsIncoming message")
            m = self.results_receiver.recv_multipart()
            logger.debug("Received ResultsIncoming message")
            return m

    def close(self):
        with self._lock:
            self.results_receiver.close()
            self.context.term()
