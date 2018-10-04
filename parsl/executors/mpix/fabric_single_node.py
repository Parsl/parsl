#!/usr/bin/env python

import argparse
import logging
import os
import sys
# import random
import threading
import pickle
import time
import queue
import uuid
import zmq

import multiprocessing

from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object
# from parsl.executors.mpix import zmq_pipes

RESULT_TAG = 10
TASK_REQUEST_TAG = 11

LOOP_SLOWDOWN = 0.0  # in seconds


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
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 worker_count=0,
                 uid=None,
                 heartbeat_period=30):
        """
        Parameters
        ----------
        worker_url : str
             Worker url on which workers will attempt to connect back

        worker_count : int,
             Workers to launch, if set to 0 we launch cores # of workers. 0 is the default
        """
        logger.info("Daimyo started v0.5")

        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.setsockopt(zmq.IDENTITY, b'00100')
        self.task_incoming.connect(task_q_url)

        self.result_outgoing = self.context.socket(zmq.DEALER)
        self.result_outgoing.setsockopt(zmq.IDENTITY, b'00100')
        self.result_outgoing.connect(result_q_url)

        self.uid = uid

        if worker_count == 0:
            self.worker_count = multiprocessing.cpu_count()
        else:
            self.worker_count = worker_count
        logger.info("Daimyo will spawn {} workers".format(self.worker_count))
        
        logger.info("Daimyo connected")
        self.pending_task_queue = multiprocessing.Queue(maxsize=self.worker_count + max_queue_size)
        self.pending_result_queue = multiprocessing.Queue(maxsize=10 ^ 4)
        self.ready_worker_queue = multiprocessing.Queue(maxsize=self.worker_count + 1)
        #self.ready_worker_counter_lock = multiprocessing.Lock()
        #self.ready_worker_counter = multiprocessing.Value('i', 0)


        if max_queue_size == 0:
            max_queue_size = self.worker_count

        self.tasks_per_round = 1

        self.heartbeat_period = heartbeat_period

    def heartbeat(self):
        """ Send heartbeat to the incoming task queue
        """
        heartbeat = (0).to_bytes(4, "little")
        r = self.task_incoming.send(heartbeat)
        logger.debug("Return from heartbeat : {}".format(r))

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
        self.heartbeat()
        last_beat = time.time()
        task_recv_counter = 0

        while not kill_event.is_set():
            time.sleep(LOOP_SLOWDOWN)
            ready_worker_count = self.ready_worker_queue.qsize()
            #ready_worker_count = self.ready_worker_counter.value
            # ready_worker_count = 2

            logger.debug("[TASK_PULL_THREAD] ready worker queue size: {}".format(ready_worker_count))

            if time.time() > last_beat + (float(self.heartbeat_period) / 2):
                self.heartbeat()
                last_beat = time.time()

            if ready_worker_count > 0:

                ready_worker_count = 4
                logger.debug("[TASK_PULL_THREAD] Requesting tasks: {}".format(ready_worker_count))
                msg = ((ready_worker_count).to_bytes(4, "little"))
                self.task_incoming.send(msg)

            # start = time.time()
            socks = dict(poller.poll(1))
            # delta = time.time() - start

            if self.task_incoming in socks and socks[self.task_incoming] == zmq.POLLIN:
                _, pkl_msg = self.task_incoming.recv_multipart()
                tasks = pickle.loads(pkl_msg)
                if tasks == 'STOP':
                    logger.critical("[TASK_PULL_THREAD] Received stop request")
                    kill_event.set()
                    break
                else:
                    logger.debug("[TASK_PULL_THREAD] Got tasks: {}".format(len(tasks)))
                    task_recv_counter += len(tasks)
                    for task in tasks:
                        self.pending_task_queue.put(task)
                        # logger.debug("[TASK_PULL_THREAD] Ready tasks : {}".format(
                        #    [i['task_id'] for i in self.pending_task_queue]))
            else:
                logger.debug("[TASK_PULL_THREAD] No incoming tasks")

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
                result = self.pending_result_queue.get(block=True, timeout=timeout)
                self.result_outgoing.send(result)
                logger.debug("[RESULT_PUSH_THREAD] Sent result:{}".format(result))

            except queue.Empty:
                logger.debug("[RESULT_PUSH_THREAD] No results to send in past {}seconds".format(timeout))

            except Exception as e:
                logger.exception("[RESULT_PUSH_THREAD] Got an exception : {}".format(e))

    def start(self):
        """ Start the Daimyo process.

        The worker loops on this:

        1. If the last message sent was older than heartbeat period we send a heartbeat
        2.


        TODO: Move task receiving to a thread
        """
        self._kill_event = threading.Event()

        self.procs = {}
        for worker_id in range(self.worker_count):
            p = multiprocessing.Process(target=worker, args=(worker_id,
                                                             self.pending_task_queue,
                                                             self.pending_result_queue,
                                                             self.ready_worker_queue,
                                                             #self.ready_worker_counter,
                                                             #self.ready_worker_counter_lock,
                                                         ))
            p.start()
            self.procs[worker_id] = p

        self._task_puller_thread = threading.Thread(target=self.pull_tasks,
                                                    args=(self._kill_event,))
        self._result_pusher_thread = threading.Thread(target=self.push_results,
                                                      args=(self._kill_event,))
        self._task_puller_thread.start()
        self._result_pusher_thread.start()


        logger.debug("Daimyo synced with workers")

        start = None
        abort_flag = False

        logger.info("Loop start")

        # TODO : Add mechanism in this loop to stop the fabric.
        # This might need a multiprocessing event to signal back.
        while not abort_flag:
            time.sleep(0.1)

        for proc_id in self.procs:
            self.procs[proc_id].terminate()

        #self._task_puller_thread.join()
        #self._result_pusher_thread.join()
        delta = time.time() - start
        logger.info("Fabric ran for {} seconds".format(delta))
        sys.exit(-1)


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


#def worker(worker_id, task_queue, result_queue, worker_counter, worker_counter_lock):
def worker(worker_id, task_queue, result_queue, worker_queue):
    """

    TODO : Add daimyo id to distinguish between multiple daimyo runs under same run

    Put request token into queue
    Get task from task_queue
    Pop request from queue
    Put result into result_queue
    """
    start_file_logger('{}/fabric_single_node_{}.log'.format(args.logdir, worker_id),
                      0,
                      level=logging.DEBUG if args.debug is True else logging.INFO)

    # Sync worker with master
    logger.info('Worker ID: {} started'.format(worker_id))

    while True:
        worker_queue.put(worker_id)
        #with worker_counter_lock:
        #    worker_counter.value += 1

        # The worker will receive {'task_id':<tid>, 'buffer':<buf>}
        req = task_queue.get()
        tid = req['task_id']
        logger.info("Got task : {}".format(tid))

        try:
            worker_queue.get(worker_id)
        except queue.Empty:
            logger.warning("Worker ID: {} failed to remove itself from ready_worker_queue".format(worker_id))
            pass
        #with worker_counter_lock:
        #    worker_counter.value -= 1

        try:
            result = execute_task(req['buffer'])

        except Exception as e:
            result_package = {'task_id': tid, 'exception': serialize_object(e)}
            logger.debug("No result due to exception: {} with result package {}".format(e, result_package))
        else:
            result_package = {'task_id': tid, 'result': serialize_object(result)}
            logger.debug("Result : {}".format(result))

        logger.info("Completed task : {}".format(tid))
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
    parser.add_argument("-l", "--logdir", default="parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Daimyo")
    parser.add_argument("-w", "--worker_count", default="0",
                        help="Number of worker processes to launch. If set to '0', launches cores # of workers. Defualt is '0'")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-r", "--result_url", required=True,
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    try:
        os.makedirs(args.logdir)
    except FileExistsError:
        pass

    # set_stream_logger()
    try:
        start_file_logger('{}/fabric_single_{}.{}.log'.format(args.logdir, args.uid, 'MAIN'),
                          0,
                          level=logging.DEBUG if args.debug is True else logging.INFO)

        logger.info("Python version :{}".format(sys.version))
        daimyo = Daimyo(task_q_url=args.task_url,
                        result_q_url=args.result_url,
                        uid=args.uid,
                        worker_count=int(args.worker_count))
        daimyo.start()
    except Exception as e:
        logger.warning("Fabric exiting")
        logger.exception("Caught error : {}".format(e))
        raise

    logger.debug("Fabric exiting")
    print("Fabric exiting.")
