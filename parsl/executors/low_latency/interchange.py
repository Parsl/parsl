#!/usr/bin/env python3

import logging
import zmq
import time

import queue
import multiprocessing as mp
from ipyparallel.serialize import pack_apply_message


ADD_EVENT = True
REMOVE_EVENT = False


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

        # Start process for monitoring tasks
        self.monitor_task_updates = mp.Queue()
        self.monitor_proc = mp.Process(target=self._task_monitor,
                                       args=(self.monitor_task_updates,))
        self.monitor_proc.start()

        logger.debug("Init complete")

    def start(self):
        """ TODO: docstring """
        logger.info("Starting interchange")

        # Measure average round-trip time to workers
        self.measure_worker_rtt()

        while True:
            socks = dict(self.poller.poll(1))

            if socks.get(self.task_incoming) == zmq.POLLIN:
                message = self.task_incoming.recv_multipart()
                logger.debug("Got new task from client")
                self.worker_messages.send_multipart(message)
                logger.debug("Sent task to worker")
                task_id = int.from_bytes(message[2], "little")
                # TODO: correct this to actual expected deadlines instead of hardcoded 1ms execution time (for testing)
                deadline = time.time() + self.worker_rtt + 1000 
                update = (ADD_EVENT, task_id, deadline)
                self.monitor_task_updates.put(update)

            if socks.get(self.worker_messages) == zmq.POLLIN:
                message = self.worker_messages.recv_multipart()
                logger.debug("Got new result from worker")
                self.result_outgoing.send_multipart(message[1:])
                logger.debug("Sent result to client")
                task_id = int.from_bytes(message[2], "little")
                update = (REMOVE_EVENT, task_id)
                self.monitor_task_updates.put(update)

    def _task_monitor(self, monitor_task_updates):
        pending_tasks = {}

        while True:
            timeout = None
            if len(pending_tasks) != 0:
                # TODO: do this faster using a heap / sorted list
                timeout = min(pending_tasks.values()) - time.time()

            try:
                # Wait until a task update is received or first timeout expires
                update = monitor_task_updates.get(True, timeout)
            except queue.Empty:  # Timeout expired
                pass
            else:  # New update event
                action = update[0]
                task_id = update[1]
                if action == ADD_EVENT:
                    pending_tasks[task_id] = update[2]
                elif action == REMOVE_EVENT:
                    del pending_tasks[task_id]
            finally:
                now = time.time()
                for task_id, deadline in pending_tasks.items():
                    if now > deadline:
                        logger.warn("Result for task {} not received on time"
                                    .format(task_id))
                        # TODO: send back error or warning for this task

    def measure_worker_rtt(self, num_measurements=10, warmup=10):
        fn_buf = pack_apply_message(lambda x: x, (0,), {},
                                    buffer_threshold=1024 * 1024,
                                    item_threshold=1024)
        task_id_bytes = (0).to_bytes(4, "little")
        message = [b"\x00", b"", task_id_bytes] + fn_buf
        times = []

        for i in range(warmup + num_measurements):
            send_time = time.time()
            self.worker_messages.send_multipart(message)
            _ = self.worker_messages.recv_multipart()
            if i >= warmup:
                times.append(time.time() - send_time)

        assert(len(times) == num_measurements)
        self.worker_rtt = sum(times) / num_measurements
        logger.info("Interchange-worker RTT is {} ms"
                    .format(self.worker_rtt * 1000))


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
    ic = Interchange(*args, **kwargs)
    comm_q.put(ic.worker_port)
    ic.start()
    logger.debug("Port information sent back to client")
