#!/usr/bin/env python

import argparse
import logging
import os
import sys
import random
# import threading
import pickle
import time
# import uuid
import zmq

from mpi4py import MPI

from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object
# from parsl.executors.mpix import zmq_pipes

RESULT_TAG = 10
TASK_REQUEST_TAG = 11


class Daimyo(object):
    """ Daimyo (feudal lord) rules over the workers

    1. Asynchronously queue large volume of tasks
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.
    """
    def __init__(self,
                 comm, rank,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 heartbeat_period=30):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back
        """
        logger.info("Daimyo started")

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, b'00100')
        self.task_incoming.connect(task_q_url)

        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, b'00100')
        self.result_outgoing.connect(result_q_url)

        logger.info("Daimyo connected")
        self.pending_task_queue = []
        self.ready_worker_queue = []
        self.max_task_queue_size = 10 ^ 5

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.comm = comm
        self.rank = rank

    def forward_result_to_interchange(self):
        """ Receives a results from the MPI fabric and send it out via 0mq
        """
        info = MPI.Status()
        result = self.comm.recv(source=MPI.ANY_SOURCE, tag=RESULT_TAG, status=info)
        self.result_outgoing.send(result)
        logger.debug("[RESULT_Q MANAGER] returned result : {}".format(result))

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (0).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat : {}".format(r))

    def recv_task_request(self):
        """ Receives 1 task request from MPI comm into the ready_worker_queue
        """
        info = MPI.Status()
        # req = comm.recv(source=MPI.ANY_SOURCE, tag=TASK_REQUEST_TAG, status=info)
        comm.recv(source=MPI.ANY_SOURCE, tag=TASK_REQUEST_TAG, status=info)
        worker_rank = info.Get_source()
        self.ready_worker_queue.append(worker_rank)
        logger.info("Received task request from worker:{}".format(worker_rank))

    def start(self):
        """ Start the Daimyo process.


        The worker loops on this:

        1. If the last message sent was older than heartbeat period we send a heartbeat
        2.


        TODO: Move task receiving to a thread
        """

        self.comm.Barrier()
        logger.debug("Daimyo synced with workers")

        count = 0
        start = None
        abort_flag = False

        # Ensure that the worker definitely sends a heartbeat at the beginning
        last_beat = 0

        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        while not abort_flag:
            logger.info("Loop start")
            time.sleep(1)
            # Probing for result
            info = MPI.Status()
            if self.comm.Iprobe(status=info):
                logger.info("There is a message waiting in MPI")
                tag = info.Get_tag()
                logger.info("Message has tag {}".format(tag))

                if tag == RESULT_TAG:
                    # recv_result(comm, result_queue)
                    self.forward_result_to_interchange()

                elif tag == TASK_REQUEST_TAG:
                    # recv_task_request(comm, self.ready_worker_queue)
                    self.recv_task_request()

                else:
                    logger.error("Unknown tag {} - ignoring this message and continuing".format(tag))

            logger.debug("Current outstanding requests : {}".format(len(self.ready_worker_queue)))
            # There are no workers waiting for tasks
            if len(self.ready_worker_queue) == 0:
                # Heartbeats are necessary only when there are no work requests being made
                if time.time() > last_beat + self.heartbeat_period:
                    self.heartbeat()
                    # heartbeat = (0).to_bytes(4, "little")
                    # r = self.task_incoming.send(heartbeat)
                    # print("Return from heartbeat : ", r)
                    last_beat = time.time()
                continue

            else:
                # There is atleast 1 worker waiting for tasks, so make a request for tasks
                # We need to be careful here to only wait for less than the heartbeat period

                # Request a specific number of tasks
                msg = (len(self.ready_worker_queue).to_bytes(4, "little"))
                print("Requesting tasks : ", len(self.ready_worker_queue))
                self.task_incoming.send(msg)
                print("Worker: Waiting to receive task")

                socks = dict(poller.poll(self.heartbeat_period / 2))
                # TODO : This bit should get the right number of tasks in 1 go
                if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:

                    # Receive a task
                    _, pkl_msg = self.task_incoming.recv_multipart()

                    tasks = pickle.loads(pkl_msg)
                    self.pending_task_queue.extend(tasks)
                    logger.debug("Ready tasks : {}".format(self.pending_task_queue))
                    # Forward to a worker
                    worker_rank = self.ready_worker_queue.pop()
                    task = self.pending_task_queue.pop()
                    comm.send(task, dest=worker_rank, tag=worker_rank)
                    time.sleep((random.randint(1, 10) / 10))

            if not start:
                start = time.time()
            # print("[{}] Received: {}".format(self.identity, msg))
            # time.sleep(random.randint(4,10)/10)
            count += 1
            if msg == 'STOP':
                break

        delta = time.time() - start
        print("Received {} tasks in {}seconds".format(count, delta))


def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the serialized result or exception.
    """
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    fname = getattr(f, '__name__', 'f')
    prefix = "parsl_"
    fname = prefix + "f"
    argname = prefix + "args"
    kwargname = prefix + "kwargs"
    resultname = prefix + "result"

    user_ns.update({fname: f,
                    argname: args,
                    kwargname: kwargs,
                    resultname: resultname})

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:
        logger.debug("[RUNNER] Executing: {0}".format(code))
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e))
        raise e

    else:
        logger.debug("[RUNNER] Result: {0}".format(user_ns.get(resultname)))
        return user_ns.get(resultname)


def worker(comm, rank):
    logger.info("Worker started")

    # Sync worker with master
    comm.Barrier()
    logger.debug("Synced")

    task_request = b'TREQ'

    while True:
        comm.send(task_request, dest=0, tag=TASK_REQUEST_TAG)
        # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
        req = comm.recv(source=0, tag=rank)
        logger.debug("Got req: {}".format(req))
        tid = req['task_id']
        logger.debug("Got task : {}".format(tid))

        try:
            result = execute_task(req['buffer'])
        except Exception as e:
            result_package = {'task_id': tid, 'exception': serialize_object(e)}
            logger.debug("No result due to exception: {} with result package {}".format(e, result_package))
        else:
            result_package = {'task_id': tid, 'result': serialize_object(result)}
            logger.debug("Result : {}".format(result))

        pkl_package = pickle.dumps(result_package)
        comm.send(pkl_package, dest=0, tag=RESULT_TAG)


def start_file_logger(filename, rank, name='parsl', level=logging.DEBUG, format_string=None):
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
        format_string = "%(asctime)s %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(rank)

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def set_stream_logger(name='parsl', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
         - name (string) : Set the logger name.
         - level (logging.LEVEL) : Set to logging.DEBUG by default.
         - format_string (sting) : Set to None by default.

    Returns:
         - None
    """
    if format_string is None:
        # format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    parser.add_argument("-l", "--logdir", default="parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-t", "--task_url",
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-r", "--result_url",
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print("Start rank :", rank)

    try:
        os.makedirs(args.logdir)
    except FileExistsError:
        pass

    set_stream_logger()
    start_file_logger('{}/mpi_rank.{}.log'.format(args.logdir, rank),
                      rank,
                      level=logging.DEBUG if args.debug is True else logging.INFO)

    logger.debug("Python version :{}".format(sys.version))

    try:
        if rank == 0:
            daimyo = Daimyo(comm, rank)
            daimyo.start()
        else:
            worker(comm, rank)
    except Exception as e:
        print("Caught error : ", e)
        raise

    print("Done")
