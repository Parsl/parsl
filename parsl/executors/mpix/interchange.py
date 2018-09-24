#!/usr/bin/env python
import argparse
import zmq
import uuid
import time
import pickle
import logging

from ipyparallel.serialize import serialize_object


class ShutdownRequest(Exception):
    ''' Error raised a required module is missing for a optional/extra provider
    '''
    def __init__(self):
        self.tstamp = time.time()

    def __repr__(self):
        return "Shutdown requet received at {}".format(self.tstamp)


class WorkerLost(Exception):
    ''' Task lost due to worker loss. Worker is considered lost when multiple heartbeats
    have been missed.
    '''
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tstamp = time.time()

    def __repr__(self):
        return "Task failure due to loss of Worker:{}".format(self.worker_id)


class Interchange(object):
    """ Interchange is a fast and reliable distributed task orchestrator

    1. Asynchronously queue large volume of tasks
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    TODO : We most likely need a PUB channel to send out global commands, like shutdown
    """
    def __init__(self,
                 client_address="127.0.0.1",
                 interchange_address="127.0.0.1",
                 client_ports=(50055, 50056),
                 interchange_ports=(50097, 50098),
                 heartbeat_period=3):
        """
        Parameters
        ----------
        client_url : str
             Client URL string

        worker_url : str
             Worker url on which workers will attempt to connect back

        heartbeat_period : int
             Heartbeat period expected from workers
        """
        logger.info("Starting Interchange process")
        self.client_address = client_address
        self.interchange_address = interchange_address
        self.identity = uuid.uuid4()

        logger.debug("Attempting connection to client at:{} port:{},{}".format(
            client_address, client_ports[0], client_ports[1]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))
        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))
        logger.debug("Connected to client")

        logger.debug("Binding to ports:{},{} for incoming worker connections".format(
            interchange_ports[0], interchange_ports[1]))
        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing.bind("tcp://*:{}".format(interchange_ports[0]))
        self.results_incoming = self.context.socket(zmq.ROUTER)
        self.results_incoming.bind("tcp://*:{}".format(interchange_ports[1]))
        logger.debug("Incoming ports bound")

        self._task_queue = []
        self._ready_worker_queue = {}
        self.max_task_queue_size = 10 ^ 5

        self.heartbeat_thresh = heartbeat_period * 2

    def get_tasks(self, count):
        """ Get's a batch of tasks from the task queue

        Parameters
        ----------
             count: int
                 Count of tasks to get from the queue

        Returns
        -------
             List of upto count tasks. May return fewer than count down to an empty list
                eg. [{'task_id':<x>, 'buffer':<buf>} ... ]

        Raises
        ------
             ShutdownRequest: If shutdown requested by client.
             Since the 'STOP' request might be queued, this may be processed in a delayed manner.

        """
        # Listen for tasks
        tasks = []
        logger.debug("Listening for {} tasks".format(count))
        for c in range(count):
            if self.task_incoming in self.socks and self.socks[self.task_incoming] == zmq.POLLIN:
                if len(self._task_queue) < self.max_task_queue_size:
                    # There's an unpickling cost here, could optimize by prepending
                    # buffer with tid
                    msg = self.task_incoming.recv_pyobj()
                    # msg = self.task_incoming.recv_string()
                    if msg == 'STOP':
                        raise ShutdownRequest
                    else:
                        tasks.append(msg)
            else:
                logger.debug("Returning with {} tasks".format(c))

        return tasks

    def start(self, poll_period=0.1):
        """ Start the NeedNameQeueu

        Parameters:
        ----------

        poll_period : float
              Poll period in seconds

        TODO: Move task receiving to a thread
        """
        # start = time.time()
        start = None
        count = 0

        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        while True:
            self.socks = dict(poller.poll(timeout=poll_period))

            # Listen for requests for work
            if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
                message = self.task_outgoing.recv_multipart()
                worker = message[0]
                tasks_requested = int.from_bytes(message[1], "little")
                worker = int.from_bytes(message[0], "little")

                logger.debug("Worker[{}] requested {} tasks".format(worker, tasks_requested))
                if worker not in self._ready_worker_queue:
                    logger.debug("Adding worker to ready queue")
                    self._ready_worker_queue[worker] = {'last': time.time(),
                                                        'wtime': 60,
                                                        'free_slots': tasks_requested,
                                                        'tasks': []}
                else:
                    self._ready_worker_queue[worker]['last'] = time.time()
                    self._ready_worker_queue[worker]['free_slots'] = tasks_requested

            # If we had received any requests, check if there are tasks that could be passed
            for worker in self._ready_worker_queue:
                if self._ready_worker_queue[worker]['free_slots']:
                    tasks = self.get_tasks(self._ready_worker_queue[worker]['free_slots'])
                    if tasks:
                        self.task_outgoing.send_multipart([message[0], b'', pickle.dumps(tasks)])
                        tids = [t['task_id'] for t in tasks]
                        logger.debug("Sent tasks: {} to {}".format(tids, worker))
                        self._ready_worker_queue[worker]['free_slots'] -= len(tasks)
                        self._ready_worker_queue[worker]['tasks'].extend(tids)
                else:
                    logger.debug("Nothing to send")

            # Receive any results and forward to client
            if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
                logger.debug("***************************")
                b_worker, b_message = self.results_incoming.recv_multipart()
                worker = int.from_bytes(b_worker, "little")
                if worker not in self._ready_worker_queue:
                    logger.debug("Received a result from a un-registered worker:{}".format(worker))
                else:
                    r = pickle.loads(b_message)
                    logger.debug("Received result for task {} from {}".format(r['task_id'], worker))
                    logger.debug("Current tasks : {}".format(self._ready_worker_queue[worker]['tasks']))
                    self._ready_worker_queue[worker]['tasks'].remove(r['task_id'])
                    self.results_outgoing.send(b_message)

            bad_workers = [worker for worker in self._ready_worker_queue if
                           time.time() - self._ready_worker_queue[worker]['last'] > self.heartbeat_thresh]
            for worker in bad_workers:
                logger.debug("Last:{} Current:{}".format(self._ready_worker_queue[worker]['last'], time.time()))
                logger.warning("Too many heartbeats missed for worker:{}".format(worker))
                e = WorkerLost(worker)
                for tid in self._ready_worker_queue[worker]['tasks']:
                    result_package = {'task_id': tid, 'exception': serialize_object(e)}
                    pkl_package = pickle.dumps(result_package)
                    self.results_outgoing.send(pkl_package)
                    logger.warning("Sent failure reports, unregistering worker")
                    self._ready_worker_queue.pop(worker, 'None')

            if not start:
                start = time.time()
            # print("[{}] Received: {}".format(self.identity, msg))
            count += 1
            # if msg == 'STOP':
            #     break

        delta = time.time() - start
        logger("Received {} tasks in {}seconds".format(count, delta))


def start_file_logger(filename, name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def starter(*args, **kwargs):
    """ MPIX is expected to start the interchange process via calling this function
    to start a new process. The args, kwargs match that of the Interchange.__init__
    """

    start_file_logger("interchange.logs")
    ic = Interchange(*args, **kwargs)
    ic.start()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client_address",
                        help="Client address")
    parser.add_argument("-l", "--logdir", default="parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-t", "--task_url",
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-r", "--result_url",
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    ic = Interchange()
    ic.start()
