#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import platform
# import random
import threading
import pickle
import time
import datetime
import queue
import uuid
import zmq
import json

from mpi4py import MPI

from parsl.app.errors import RemoteExceptionWrapper
from parsl.version import VERSION as PARSL_VERSION
from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object

RESULT_TAG = 10
TASK_REQUEST_TAG = 11

LOOP_SLOWDOWN = 0.0  # in seconds

HEARTBEAT_CODE = (2 ** 32) - 1


class Manager(object):
    """ Orchestrates the flow of tasks and results to and from the workers

    1. Queue up task requests from workers
    2. Make batched requests from to the interchange for tasks
    3. Receive and distribute tasks to workers
    4. Act as a proxy to the Interchange for results.
    """
    def __init__(self,
                 comm, rank,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 uid=None):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        heartbeat_threshold : int
             Number of seconds since the last message from the interchange after which the worker
             assumes that the interchange is lost and the manager shuts down. Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the interchange

        """
        self.uid = uid

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        # Linger is set to 0, so that the manager can exit even when there might be
        # messages in the pipe
        self.task_incoming.setsockopt(zmq.LINGER, 0)
        self.task_incoming.connect(task_q_url)

        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, uid.encode('utf-8'))
        self.result_outgoing.setsockopt(zmq.LINGER, 0)
        self.result_outgoing.connect(result_q_url)

        logger.info("Manager connected")
        self.max_queue_size = max_queue_size + comm.size

        # Creating larger queues to avoid queues blocking
        # These can be updated after queue limits are better understood
        self.pending_task_queue = queue.Queue()
        self.pending_result_queue = queue.Queue()
        self.ready_worker_queue = queue.Queue()

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.comm = comm
        self.rank = rank

    def create_reg_message(self):
        """ Creates a registration message to identify the worker to the interchange
        """
        msg = {'parsl_v': PARSL_VERSION,
               'python_v': "{}.{}.{}".format(sys.version_info.major,
                                             sys.version_info.minor,
                                             sys.version_info.micro),
               'os': platform.system(),
               'hostname': platform.node(),
               'dir': os.getcwd(),
               'prefetch_capacity': 0,
               'worker_count': (self.comm.size - 1),
               'max_capacity': (self.comm.size - 1) + 0,  # (+prefetch)
               'reg_time': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        b_msg = json.dumps(msg).encode('utf-8')
        return b_msg

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (HEARTBEAT_CODE).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat : {}".format(r))

    def recv_result_from_workers(self):
        """ Receives a results from the MPI worker pool and send it out via 0mq

        Returns:
        --------
            result: task result from the workers
        """
        info = MPI.Status()
        result = self.comm.recv(source=MPI.ANY_SOURCE, tag=RESULT_TAG, status=info)
        logger.debug("Received result from workers: {}".format(result))
        return result

    def recv_task_request_from_workers(self):
        """ Receives 1 task request from MPI comm

        Returns:
        --------
            worker_rank: worker_rank id
        """
        info = MPI.Status()
        comm.recv(source=MPI.ANY_SOURCE, tag=TASK_REQUEST_TAG, status=info)
        worker_rank = info.Get_source()
        logger.info("Received task request from worker:{}".format(worker_rank))
        return worker_rank

    def pull_tasks(self, kill_event):
        """ Pulls tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK PULL THREAD] starting")
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        # Send a registration message
        msg = self.create_reg_message()
        logger.debug("Sending registration message: {}".format(msg))
        self.task_incoming.send(msg)
        last_beat = time.time()
        last_interchange_contact = time.time()
        task_recv_counter = 0

        poll_timer = 1

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            ready_worker_count = self.ready_worker_queue.qsize()
            pending_task_count = self.pending_task_queue.qsize()

            logger.debug("[TASK_PULL_THREAD] ready workers:{}, pending tasks:{}".format(ready_worker_count,
                                                                                        pending_task_count))

            if time.time() > last_beat + self.heartbeat_period:
                self.heartbeat()
                last_beat = time.time()

            if pending_task_count < self.max_queue_size and ready_worker_count > 0:
                logger.debug("[TASK_PULL_THREAD] Requesting tasks: {}".format(ready_worker_count))
                msg = ((ready_worker_count).to_bytes(4, "little"))
                self.task_incoming.send(msg)

            socks = dict(poller.poll(timeout=poll_timer))

            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                _, pkl_msg = self.task_incoming.recv_multipart()
                tasks = pickle.loads(pkl_msg)
                last_interchange_contact = time.time()

                if tasks == 'STOP':
                    logger.critical("[TASK_PULL_THREAD] Received stop request")
                    kill_event.set()
                    break

                elif tasks == HEARTBEAT_CODE:
                    logger.debug("Got heartbeat from interchange")

                else:
                    # Reset timer on receiving message
                    poll_timer = 1
                    task_recv_counter += len(tasks)
                    logger.debug("[TASK_PULL_THREAD] Got tasks: {} of {}".format([t['task_id'] for t in tasks],
                                                                                 task_recv_counter))
                    for task in tasks:
                        self.pending_task_queue.put(task)
            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                poll_timer = min(self.heartbeat_period * 1000, poll_timer * 2)

                # Only check if no messages were received.
                if time.time() > last_interchange_contact + self.heartbeat_threshold:
                    logger.critical("[TASK_PULL_THREAD] Missing contact with interchange beyond heartbeat_threshold")
                    kill_event.set()
                    logger.critical("[TASK_PULL_THREAD] Exiting")
                    break

    def push_results(self, kill_event):
        """ Listens on the pending_result_queue and sends out results via 0mq

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """

        # We set this timeout so that the thread checks the kill_event and does not
        # block forever on the internal result queue
        timeout = 0.1
        # timer = time.time()
        logger.debug("[RESULT_PUSH_THREAD] Starting thread")

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            try:
                items = []
                while not self.pending_result_queue.empty():
                    r = self.pending_result_queue.get(block=True)
                    items.append(r)
                if items:
                    self.result_outgoing.send_multipart(items)

            except queue.Empty:
                logger.debug("[RESULT_PUSH_THREAD] No results to send in past {}seconds".format(timeout))

            except Exception as e:
                logger.exception("[RESULT_PUSH_THREAD] Got an exception : {}".format(e))

        logger.critical("[RESULT_PUSH_THREAD] Exiting")

    def start(self):
        """ Start the Manager process.

        The worker loops on this:

        1. If the last message sent was older than heartbeat period we send a heartbeat
        2.


        TODO: Move task receiving to a thread
        """

        self.comm.Barrier()
        logger.debug("Manager synced with workers")

        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(target=self.pull_tasks,
                                                    args=(self._kill_event,))
        self._result_pusher_thread = threading.Thread(target=self.push_results,
                                                      args=(self._kill_event,))
        self._task_puller_thread.start()
        self._result_pusher_thread.start()

        start = None

        result_counter = 0
        task_recv_counter = 0
        task_sent_counter = 0

        logger.info("Loop start")
        while not self._kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)

            # In this block we attempt to probe MPI for a set amount of time,
            # and if we have exhausted all available MPI events, we move on
            # to the next block. The timer and counter trigger balance
            # fairness and responsiveness.
            timer = time.time() + 0.05
            counter = min(10, comm.size)
            while time.time() < timer:
                info = MPI.Status()

                if counter > 10:
                    logger.debug("Hit max mpi events per round")
                    break

                if not self.comm.Iprobe(status=info):
                    logger.debug("Timer expired, processed {} mpi events".format(counter))
                    break
                else:
                    tag = info.Get_tag()
                    logger.info("Message with tag {} received".format(tag))

                    counter += 1
                    if tag == RESULT_TAG:
                        result = self.recv_result_from_workers()
                        self.pending_result_queue.put(result)
                        result_counter += 1

                    elif tag == TASK_REQUEST_TAG:
                        worker_rank = self.recv_task_request_from_workers()
                        self.ready_worker_queue.put(worker_rank)

                    else:
                        logger.error("Unknown tag {} - ignoring this message and continuing".format(tag))

            available_worker_cnt = self.ready_worker_queue.qsize()
            available_task_cnt = self.pending_task_queue.qsize()
            logger.debug("[MAIN] Ready workers: {} Ready tasks: {}".format(available_worker_cnt,
                                                                           available_task_cnt))
            this_round = min(available_worker_cnt, available_task_cnt)
            for i in range(this_round):
                worker_rank = self.ready_worker_queue.get()
                task = self.pending_task_queue.get()
                comm.send(task, dest=worker_rank, tag=worker_rank)
                task_sent_counter += 1
                logger.debug("Assigning worker:{} task:{}".format(worker_rank, task['task_id']))

            if not start:
                start = time.time()

            logger.debug("Tasks recvd:{} Tasks dispatched:{} Results recvd:{}".format(
                task_recv_counter, task_sent_counter, result_counter))
            # print("[{}] Received: {}".format(self.identity, msg))
            # time.sleep(random.randint(4,10)/10)

        self._task_puller_thread.join()
        self._result_pusher_thread.join()

        self.task_incoming.close()
        self.result_outgoing.close()
        self.context.term()

        delta = time.time() - start
        logger.info("mpi_worker_pool ran for {} seconds".format(delta))


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
        logger.debug("Got task: {}".format(tid))

        try:
            result = execute_task(req['buffer'])
        except Exception as e:
            result_package = {'task_id': tid, 'exception': serialize_object(RemoteExceptionWrapper(*sys.exc_info()))}
            logger.debug("No result due to exception: {} with result package {}".format(e, result_package))
        else:
            result_package = {'task_id': tid, 'result': serialize_object(result)}
            logger.debug("Result: {}".format(result))

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
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d Rank:{0} [%(levelname)s]  %(message)s".format(rank)

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
        format_string = "%(asctime)s %(name)s [%(levelname)s] Thread:%(thread)d %(message)s"
        # format_string = "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

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
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Manager")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("--hb_period", default=30,
                        help="Heartbeat period in seconds. Uses manager default unless set")
    parser.add_argument("--hb_threshold", default=120,
                        help="Heartbeat threshold in seconds. Uses manager default unless set")
    parser.add_argument("-r", "--result_url", required=True,
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    print("Starting rank: {}".format(rank))

    os.makedirs(args.logdir, exist_ok=True)

    # set_stream_logger()
    try:
        if rank == 0:
            start_file_logger('{}/manager.mpi_rank_{}.log'.format(args.logdir, rank),
                              rank,
                              level=logging.DEBUG if args.debug is True else logging.INFO)

            logger.info("Python version: {}".format(sys.version))

            manager = Manager(comm, rank,
                              task_q_url=args.task_url,
                              result_q_url=args.result_url,
                              uid=args.uid,
                              heartbeat_threshold=int(args.hb_threshold),
                              heartbeat_period=int(args.hb_period))
            manager.start()
            logger.debug("Finalizing MPI Comm")
            comm.Abort()
        else:
            start_file_logger('{}/worker.mpi_rank_{}.log'.format(args.logdir, rank),
                              rank,
                              level=logging.DEBUG if args.debug is True else logging.INFO)
            worker(comm, rank)
    except Exception as e:
        logger.critical("mpi_worker_pool exiting from an exception")
        logger.exception("Caught error: {}".format(e))
        raise
    else:
        logger.info("mpi_worker_pool exiting")
        print("MPI_WORKER_POOL exiting.")
