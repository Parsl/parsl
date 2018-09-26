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
                 max_task_queue_size=10,
                 heartbeat_period=30):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back
        """
        logger.info("Daimyo started v0.4")

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
        self.max_task_queue_size = max_task_queue_size

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

        result_counter = 0
        task_recv_counter = 0
        task_sent_counter = 0
        while not abort_flag:
            logger.info("Loop start")
            time.sleep(0.01)

            # In this block we attempt to probe MPI for a set amount of time,
            # and if we have exhausted all available MPI events, we move on
            # to the next block. The timer and counter trigger balance
            # fairness and responsiveness.
            timer = time.time() + 0.05
            counter = 0
            while time.time() < timer :
                info = MPI.Status()
                if not self.comm.Iprobe(status=info):
                    logger.debug("Timer expired, processed {} mpi events".format(counter))
                    break
                else:
                    tag = info.Get_tag()
                    logger.info("Message with tag {} received".format(tag))

                    counter += 1
                    if tag == RESULT_TAG:
                        # recv_result(comm, result_queue)
                        self.forward_result_to_interchange()
                        result_counter+=1
                        logger.debug("Forwarded result for task count {}".format(result_counter))

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

                items = len(self.ready_worker_queue)
                # Request a specific number of tasks
                msg = ((items).to_bytes(4, "little"))
                # msg = ((self.max_task_queue_size).to_bytes(4, "little"))
                # msg = (len(self.ready_worker_queue).to_bytes(4, "little"))
                #print("Requesting tasks : ", len(self.ready_worker_queue))
                self.task_incoming.send(msg)
                #print("Worker: Waiting to receive task")

                start = time.time()
                # socks = dict(poller.poll(timeout=(self.heartbeat_period / 2)))
                socks = dict(poller.poll(1))
                delta = time.time() - start
                logger.debug("Time taken for poll: {}".format(delta))
                # TODO : This bit should get the right number of tasks in 1 go
                if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                    # Receive a task
                    _, pkl_msg = self.task_incoming.recv_multipart()
                    tasks = pickle.loads(pkl_msg)
                    task_recv_counter += len(tasks)
                    self.pending_task_queue.extend(tasks)
                    logger.debug("Ready tasks : {}".format([i['task_id'] for i in self.pending_task_queue]))

            this_round = min(len(self.ready_worker_queue), len(self.pending_task_queue))
            for i in range(this_round):
                worker_rank = self.ready_worker_queue.pop()
                task = self.pending_task_queue.pop()
                comm.send(task, dest=worker_rank, tag=worker_rank)
                task_sent_counter += 1
                logger.debug("Assigning Worker:{} task:{}".format(worker_rank, task['task_id']))


            if not start:
                start = time.time()

            logger.debug("Tasks recvd:{} Tasks dispatched:{} Results recvd:{}".format(
                task_recv_counter, task_sent_counter, result_counter))
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


    start_file_logger('{}/mpi_rank.{}.log'.format(args.logdir, rank),
                      rank,
                      level=logging.DEBUG if args.debug is True else logging.INFO)

    try:
        if rank == 0:
            logger.info("Python version :{}".format(sys.version))
            daimyo = Daimyo(comm, rank)
            daimyo.start()
        else:
            worker(comm, rank)
    except Exception as e:
        logger.warning("Fabric exiting")
        logger.exception("Caught error : {}".format(e))
        raise

    print("Done")
