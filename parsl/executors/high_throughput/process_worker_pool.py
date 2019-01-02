#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import sys
import platform
# import random
import threading
import pickle
import time
import queue
import uuid
import zmq
import math
import json

from parsl.version import VERSION as PARSL_VERSION
import multiprocessing

from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object

RESULT_TAG = 10
TASK_REQUEST_TAG = 11

LOOP_SLOWDOWN = 0.00  # in seconds

HEARTBEAT_CODE = (2 ** 32) - 1


class Manager(object):
    """ Manager manages task execution by the workers

                |         0mq              |    Manager         |   Worker Processes
                |                          |                    |
                | <-----Request N task-----+--Count task reqs   |      Request task<--+
    Interchange | -------------------------+->Receive task batch|          |          |
                |                          |  Distribute tasks--+----> Get(block) &   |
                |                          |                    |      Execute task   |
                |                          |                    |          |          |
                | <------------------------+--Return results----+----  Post result    |
                |                          |                    |          |          |
                |                          |                    |          +----------+
                |                          |                IPC-Qeueues

    """
    def __init__(self,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 cores_per_worker=1,
                 uid=None,
                 heartbeat_threshold=120,
                 heartbeat_period=30):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        uid : str
             string unique identifier

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        heartbeat_threshold : int
             Seconds since the last message from the interchange after which the
             interchange is assumed to be un-available, and the manager initiates shutdown. Default:120s

             Number of seconds since the last message from the interchange after which the worker
             assumes that the interchange is lost and the manager shuts down. Default:120

        heartbeat_period : int
             Number of seconds after which a heartbeat message is sent to the interchange

        """
        logger.info("Manager started")

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

        self.uid = uid

        cores_on_node = multiprocessing.cpu_count()
        self.worker_count = math.floor(cores_on_node / cores_per_worker)
        logger.info("Manager will spawn {} workers".format(self.worker_count))

        self.pending_task_queue = multiprocessing.Queue(maxsize=self.worker_count + max_queue_size)
        self.pending_result_queue = multiprocessing.Queue()
        self.ready_worker_queue = multiprocessing.Queue(maxsize=self.worker_count + 1)

        self.max_queue_size = max_queue_size + self.worker_count

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold

    def create_reg_message(self):
        """ Creates a registration message to identify the worker to the interchange
        """
        msg = {'parsl_v': PARSL_VERSION,
               'python_v': "{}.{}.{}".format(sys.version_info.major,
                                             sys.version_info.minor,
                                             sys.version_info.micro),
               'os': platform.system(),
               'hname': platform.node(),
               'dir': os.getcwd(),
        }
        b_msg = json.dumps(msg).encode('utf-8')
        return b_msg

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (HEARTBEAT_CODE).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat: {}".format(r))

    def pull_tasks(self, kill_event):
        """ Pull tasks from the incoming tasks 0mq pipe onto the internal
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

        poll_timer = 0

        while not kill_event.is_set():
            # time.sleep(LOOP_SLOWDOWN)
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
                poll_timer = 0
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
                    task_recv_counter += len(tasks)
                    logger.debug("[TASK_PULL_THREAD] Got tasks: {} of {}".format([t['task_id'] for t in tasks],
                                                                                 task_recv_counter))

                    for task in tasks:
                        self.pending_task_queue.put(task)
                        # logger.debug("[TASK_PULL_THREAD] Ready tasks: {}".format(
                        #    [i['task_id'] for i in self.pending_task_queue]))

            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")
                # Limit poll duration to heartbeat_period
                # heartbeat_period is in s vs poll_timer in ms
                if not poll_timer:
                    poll_timer = 1
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

        logger.debug("[RESULT_PUSH_THREAD] Starting thread")

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            items = []
            try:
                while not self.pending_result_queue.empty():
                    r = self.pending_result_queue.get(block=True)
                    items.append(r)

                if items:
                    self.result_outgoing.send_multipart(items)

            except queue.Empty:
                pass

            except Exception as e:
                logger.exception("[RESULT_PUSH_THREAD] Got an exception: {}".format(e))

        logger.critical("[RESULT_PUSH_THREAD] Exiting")

    def start(self):
        """ Start the worker processes.

        TODO: Move task receiving to a thread
        """
        start = time.time()
        self._kill_event = threading.Event()

        self.procs = {}
        for worker_id in range(self.worker_count):
            p = multiprocessing.Process(target=worker, args=(worker_id,
                                                             self.uid,
                                                             self.pending_task_queue,
                                                             self.pending_result_queue,
                                                             self.ready_worker_queue,
                                                         ))
            p.start()
            self.procs[worker_id] = p

        logger.debug("Manager synced with workers")

        self._task_puller_thread = threading.Thread(target=self.pull_tasks,
                                                    args=(self._kill_event,))
        self._result_pusher_thread = threading.Thread(target=self.push_results,
                                                      args=(self._kill_event,))
        self._task_puller_thread.start()
        self._result_pusher_thread.start()

        logger.info("Loop start")

        # TODO : Add mechanism in this loop to stop the worker pool
        # This might need a multiprocessing event to signal back.
        while not self._kill_event.is_set():
            time.sleep(0.1)
        logger.critical("[MAIN] Received kill event, terminating worker processes")

        self._task_puller_thread.join()
        self._result_pusher_thread.join()
        for proc_id in self.procs:
            self.procs[proc_id].terminate()
            logger.critical("Terminating worker {}:{}".format(self.procs[proc_id],
                                                              self.procs[proc_id].is_alive()))
            self.procs[proc_id].join()
            logger.debug("Worker:{} joined successfully".format(self.procs[proc_id]))

        self.task_incoming.close()
        self.result_outgoing.close()
        self.context.term()
        delta = time.time() - start
        logger.info("process_worker_pool ran for {} seconds".format(delta))
        return


def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    # We might need to look into callability of the function from itself
    # since we change it's name in the new namespace
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
        # logger.debug("[RUNNER] Executing: {0}".format(code))
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e), exc_info=True)
        raise e

    else:
        # logger.debug("[RUNNER] Result: {0}".format(user_ns.get(resultname)))
        return user_ns.get(resultname)


def worker(worker_id, pool_id, task_queue, result_queue, worker_queue):
    """

    Put request token into queue
    Get task from task_queue
    Pop request from queue
    Put result into result_queue
    """
    start_file_logger('{}/{}/worker_{}.log'.format(args.logdir, pool_id, worker_id),
                      worker_id,
                      name="worker_log",
                      level=logging.DEBUG if args.debug else logging.INFO)

    # Sync worker with master
    logger.info('Worker {} started'.format(worker_id))
    if args.debug:
        logger.debug("Debug logging enabled")

    while True:
        worker_queue.put(worker_id)

        # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
        req = task_queue.get()
        tid = req['task_id']
        logger.info("Received task {}".format(tid))

        try:
            worker_queue.get()
        except queue.Empty:
            logger.warning("Worker ID: {} failed to remove itself from ready_worker_queue".format(worker_id))
            pass

        try:
            result = execute_task(req['buffer'])
            serialized_result = serialize_object(result)
        except Exception as e:
            result_package = {'task_id': tid, 'exception': serialize_object("Exception which we cannot send the full exception object back for: {}".format(e))}
        else:
            result_package = {'task_id': tid, 'result': serialized_result}
            # logger.debug("Result: {}".format(result))

        logger.info("Completed task {}".format(tid))
        pkl_package = pickle.dumps(result_package)

        result_queue.put(pkl_package)


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
    parser.add_argument("-l", "--logdir", default="process_worker_pool_logs",
                        help="Process worker pool log directory")
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Manager")
    parser.add_argument("-c", "--cores_per_worker", default="1.0",
                        help="Number of cores assigned to each worker process. Default=1.0")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("--hb_period", default=30,
                        help="Heartbeat period in seconds. Uses manager default unless set")
    parser.add_argument("--hb_threshold", default=120,
                        help="Heartbeat threshold in seconds. Uses manager default unless set")
    parser.add_argument("-r", "--result_url", required=True,
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    try:
        os.makedirs(os.path.join(args.logdir, args.uid))
    except FileExistsError:
        pass

    try:
        start_file_logger('{}/{}/manager.log'.format(args.logdir, args.uid),
                          0,
                          level=logging.DEBUG if args.debug is True else logging.INFO)

        set_stream_logger()
        logger.info("Python version: {}".format(sys.version))
        logger.info("Debug logging: {}".format(args.debug))
        logger.info("Log dir: {}".format(args.logdir))
        logger.info("Manager ID: {}".format(args.uid))
        logger.info("cores_per_worker: {}".format(args.cores_per_worker))
        logger.info("task_url: {}".format(args.task_url))
        logger.info("result_url: {}".format(args.result_url))

        manager = Manager(task_q_url=args.task_url,
                          result_q_url=args.result_url,
                          uid=args.uid,
                          cores_per_worker=float(args.cores_per_worker),
                          heartbeat_threshold=int(args.hb_threshold),
                          heartbeat_period=int(args.hb_period))
        manager.start()

    except Exception as e:
        logger.critical("process_worker_pool exiting from an exception")
        logger.exception("Caught error: {}".format(e))
        raise
    else:
        logger.info("process_worker_pool exiting")
        print("PROCESS_WORKER_POOL exiting")
