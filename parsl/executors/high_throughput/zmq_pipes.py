#!/usr/bin/env python3

import zmq
import time
import pickle


class TasksOutgoing(object):
    """ Outgoing task queue from MPIX
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
        self.port = self.zmq_socket.bind_to_random_port("tcp://{}".format(ip_address),
                                                        min_port=port_range[0],
                                                        max_port=port_range[1])

    def put(self, message):
        self.zmq_socket.send_pyobj(message)

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class ResultsIncoming(object):

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
        self.port = self.results_receiver.bind_to_random_port("tcp://{}".format(ip_address),
                                                              min_port=port_range[0],
                                                              max_port=port_range[1])

    def get(self, block=True, timeout=None):
        result = self.results_receiver.recv_pyobj()
        return result

    def request_close(self):
        status = self.results_receiver.send(pickle.dumps(None))
        time.sleep(0.1)
        return status

    def close(self):
        self.results_receiver.close()
        self.context.term()
