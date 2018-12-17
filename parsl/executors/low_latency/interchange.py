#!/usr/bin/env python3

import logging
import zmq
# import time


class Interchange(object):
    """ TODO: docstring """

    def __init__(self,
                 client_address="127.0.0.1",
                 client_ports=(50055, 50056),
                 worker_port=None,
                 worker_port_range=(54000, 55000)
                 ):
        global logger
        start_file_logger("interchange.log")
        logger.info("Init Interchange")

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.ROUTER)
        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.worker_messages = self.context.socket(zmq.DEALER)

        self.result_outgoing.set_hwm(0)

        task_address = "tcp://{}:{}".format(client_address, client_ports[0])
        result_address = "tcp://{}:{}".format(client_address, client_ports[1])
        self.task_incoming.connect(task_address)
        self.result_outgoing.connect(result_address)

        logger.debug("Client task address: {}".format(task_address))
        logger.debug("Client result address: {}".format(result_address))

        self.worker_port = worker_port
        self.worker_port_range = worker_port_range

        if self.worker_port:
            worker_task_address = "tcp://*:{}".format(self.worker_port)
            self.worker_messages.bind(worker_task_address)
            logger.debug("Worker task address: {}".format(worker_task_address))

        else:
            self.worker_port = self.worker_messages.bind_to_random_port(
                'tcp://*',
                min_port=worker_port_range[0],
                max_port=worker_port_range[1], max_tries=100)

            logger.debug(
                "Worker task address: tcp://*:{}".format(self.worker_port))

        self.poller = zmq.Poller()
        self.poller.register(self.task_incoming, zmq.POLLIN)
        self.poller.register(self.worker_messages, zmq.POLLIN)
        logger.debug("Init complete")

    def start(self):
        """ TODO: docstring """
        logger.info("Starting interchange")
        # last = time.time()

        while True:
            # active_flag = False
            socks = dict(self.poller.poll(1))

            if socks.get(self.task_incoming) == zmq.POLLIN:
                message = self.task_incoming.recv_multipart()
                logger.debug("Got new task from client")
                self.worker_messages.send_multipart(message)
                logger.debug("Sent task to worker")
                # active_flag = True
                # last = time.time()

            if socks.get(self.worker_messages) == zmq.POLLIN:
                message = self.worker_messages.recv_multipart()
                logger.debug("Got new result from worker")
                # self.result_outgoing.send_multipart(message)
                self.result_outgoing.send_multipart(message[1:])

                logger.debug("Sent result to client")
                # active_flag = True
                # last = time.time()

            # if not active_flag and last + 1 < time.time():
            #    logger.debug("Nothing in the past 1s round")
            #    last = time.time()


def start_file_logger(filename, name='interchange', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name. Default="parsl.executors.interchange"
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.

    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function. The args, kwargs match that of the Interchange.__init__
    """
    # logger = multiprocessing.get_logger()
    ic = Interchange(*args, **kwargs)
    comm_q.put(ic.worker_port)
    ic.start()
    logger.debug("Port information sent back to client")
