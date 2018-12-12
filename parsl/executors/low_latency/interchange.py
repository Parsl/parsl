#!/usr/bin/env python3

# import logging

import zmq


class Interchange(object):
    """ TODO: docstring """
    def __init__(self, 
                 client_address="127.0.0.1",
                 client_ports=(50055, 50056, 50057),
                 worker_ports=None,
                 worker_port_range=(54000, 55000)
                ):
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.ROUTER)
        self.result_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing = self.context.socket(zmq.DEALER)
        self.result_incoming = self.context.socket(zmq.DEALER)

        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))
        self.result_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind("tcp://*:{}".format(self.worker_task_port))
            self.result_incoming.bind("tcp://*:{}".format(self.worker_result_port))

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port('tcp://*',
                                                                           min_port=worker_port_range[0],
                                                                           max_port=worker_port_range[1], max_tries=100)
            self.worker_result_port = self.result_incoming.bind_to_random_port('tcp://*',
                                                                                min_port=worker_port_range[0],
                                                                                max_port=worker_port_range[1], max_tries=100)

        self.poller = zmq.Poller()
        self.poller.register(self.task_incoming, zmq.POLLIN)
        self.poller.register(self.result_incoming, zmq.POLLIN)

    def start(self):
        """ TODO: docstring """
        print("Starting interchange")

        while True:
            socks = dict(self.poller.poll(1))

            if socks.get(self.task_incoming) == zmq.POLLIN:
                message = self.task_incoming.recv_multipart()
                print("Got new task from client")
                self.task_outgoing.send_multipart(message)
                print("Sent task to worker")

            if socks.get(self.result_incoming) == zmq.POLLIN:
                message = self.result_incoming.recv_multipart()
                print("Got new result from worker")
                self.result_outgoing.send_multipart(message)
                print("Sent result to client")


def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function. The args, kwargs match that of the Interchange.__init__
    """
    # logger = multiprocessing.get_logger()
    ic = Interchange(*args, **kwargs)
    comm_q.put((ic.worker_task_port,
                ic.worker_result_port))
    ic.start()
