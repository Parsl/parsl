#!/usr/bin/env python3

import zmq
import logging

logger = logging.getLogger(__name__)


class TasksOutgoing(object):
    """ TODO: docstring """
    def __init__(self, ip_address, port_range):
        """ TODO: docstring """
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.DEALER)
        self.port = self.zmq_socket.bind_to_random_port("tcp://{}".format(ip_address),
                                                        min_port=port_range[0],
                                                        max_port=port_range[1])
        self.poller = zmq.Poller()
        self.poller.register(self.zmq_socket, zmq.POLLOUT)

    def put(self, task_id, buffer):
        """ TODO: docstring """
        print("TasksOutgoing.put called")
        task_id_bytes = task_id.to_bytes(4, "little")
        message = [b"", task_id_bytes] + buffer

        timeout_ms = 0
        while True:
            try:
                socks = dict(self.poller.poll(timeout=timeout_ms))
                if self.zmq_socket in socks and socks[self.zmq_socket] == zmq.POLLOUT:
                    self.zmq_socket.send_multipart(message)
                    logger.debug("Sent task {}".format(task_id))
                    return
                else:
                    timeout_ms += 1
                    print("Not sending due full zmq pipe, timeout: {} ms"
                                 .format(timeout_ms))
            except Exception as e:
                logger.error("Caught exception : {}".format(e))
                raise

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class ResultsIncoming(object):
    """ TODO: docstring """
    def __init__(self, ip_address, port_range):
        """ TODO: docstring """
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.DEALER)
        self.port = self.zmq_socket.bind_to_random_port("tcp://{}".format(ip_address),
                                                        min_port=port_range[0],
                                                        max_port=port_range[1])

    def get(self):
        print("ResultsIncoming.get called")
        result = self.zmq_socket.recv_multipart()
        print("Got something!")
        task_id = int.from_bytes(result[1], "little")
        buffer = result[2:]
        return task_id, buffer

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class TasksIncoming(object):
    """ TODO: docstring """
    def __init__(self, tasks_url):
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.REP)
        self.zmq_socket.connect(tasks_url)

    def get(self):
        bufs = self.zmq_socket.recv_multipart()
        task_id = int.from_bytes(bufs[0], "little")
        return task_id, bufs[1:]

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class ResultsOutgoing(object):
    """ TODO: docstring """
    def __init__(self, results_url):
        self.context = zmq.Context()
        self.zmq_socket = self.context.socket(zmq.REP)
        self.zmq_socket.connect(results_url)

    def put(self, task_id, buffer):
        task_id_bytes = task_id.to_bytes(4, "little")
        self.zmq_socket.send_multipart([task_id_bytes] + buffer)

    def close(self):
        self.zmq_socket.close()
        self.context.term()
