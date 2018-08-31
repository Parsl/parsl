#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import threading
import time
import zmq

from mpi4py import MPI

from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object
from parsl.executors.mpix import zmq_pipes

RESULT_TAG = 10
TASK_REQUEST_TAG = 11
HEARTBEAT_PERIOD = 30  # Seconds


def result_queue_manager(comm, result_q, worker_catalog, fabric_id):
    """ Manages all responses from workers

    The result queue uses MPI Iprobe to asynchronously check for messages matching
    the RESULT_TAG and when one is available forwards it to the outgoing result queue.

    In addition there's a 30s timer which is used to send a heartbeat message to the
    MPIExecutor, which contains the list of active tasks. Without locking there's a
    potential race condition since active_tasks could change while the heart_beat
    message is being made. We could probably send a dummy message back via results_q
    on receipt on the task.
    """

    logger.debug("Starting result_queue_manager thread")

    hbt_timer = time.time()

    while True:
        info = MPI.Status()
        while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=RESULT_TAG, status=info):
            time.sleep(0.1)
            delta = time.time() - hbt_timer
            if delta > 30:
                logger.debug("Sending heartbeat")
                result_q.put({'hbt': time.time(),
                              'fabric_id': fabric_id,
                              'active_tasks': [task for task in worker_catalog.values() if task]})
                hbt_timer = time.time()

        incoming_rank = info.Get_source()
        result = comm.recv(source=incoming_rank, tag=RESULT_TAG, status=info)
        worker_catalog[incoming_rank] = None
        logger.debug("[RESULT_Q MANAGER] Received result for task:{}".format(result['task_id']))
        result_q.put(result)

        # Make sure the heartbeat is sent even when the results are flowing back quickly
        delta = time.time() - hbt_timer
        if delta > HEARTBEAT_PERIOD:
            logger.debug("Sending heartbeat")
            result_q.put({'hbt': time.time(),
                          'fabric_id': fabric_id,
                          'active_tasks': [task for task in worker_catalog.values() if task]})
            hbt_timer = time.time()


def master(comm, rank, fabric_id=None, task_q_url=None, result_q_url=None):
    """ Due to the asynchronous nature of the the task queue and results queue
    we have the main thread processing requests for jobs and the a secondary thread
    that exclusively listens for results using tags
    """

    logger.info("Master started")

    task_queue = zmq_pipes.JobsQIncoming(task_q_url, server_id=fabric_id)
    result_queue = zmq_pipes.ResultsQOutgoing(result_q_url, server_id=fabric_id)

    logger.info("Connected to task_queue:{}".format(task_queue))
    logger.info("Connected to result_queue:{}".format(result_queue))

    # Sync everything
    comm.Barrier()
    logger.debug("Master synced")

    # The catalog assumes rank:0 is master, and the remainder are workers
    worker_catalog = {i: None for i in range(1, comm.size)}

    # Starting threads to listen for results
    result_queue_thread = threading.Thread(target=result_queue_manager,
                                           args=(comm, result_queue,
                                                 worker_catalog, fabric_id))
    result_queue_thread.daemon = True
    result_queue_thread.start()

    count = 1
    start = time.time()
    abort_flag = False

    while True:

        try:
            task = task_queue.get(timeout=10)

        except zmq.Again as e:
            # logger.debug("No tasks yet: {}".format(e))
            time.sleep(0.1)
            continue

        except Exception as e:
            logger.debug("Caught task recv exception : {}".format(e))
            time.sleep(0.1)
            continue

        logger.debug("Received task:{}".format(task['task_id']))

        if task["task_id"] == "STOP":
            logger.info("Received STOP request, preparing to terminate MPI fabric")
            abort_flag = True
            break
        else:
            tid = task["task_id"]

        info = MPI.Status()
        comm.recv(source=MPI.ANY_SOURCE, tag=TASK_REQUEST_TAG, status=info)
        worker_rank = info.Get_source()
        logger.info("Assigning task:{} to rank:{}".format(task["task_id"], worker_rank))
        comm.send(task, dest=worker_rank, tag=worker_rank)

        result_queue.put({'task_id': tid,
                          'fabric_id': fabric_id,
                          'info': time.time()})

        worker_catalog[worker_rank] = tid
        count += 1

    end = time.time()
    rate = float(count) / (end - start)
    logger.warn("Total count:{} Task rate:{}".format(count, rate))

    if abort_flag:
        comm.Abort()


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


def worker(comm, rank, fabric_id=None):
    logger.info("Worker started")

    # Sync worker with master
    comm.Barrier()
    logger.debug("Synced")

    task_request = b'TREQ'

    snd_req = None

    while True:
        comm.send(task_request, dest=0, tag=TASK_REQUEST_TAG)
        req = comm.recv(source=0, tag=rank)

        tid = req['task_id']
        logger.debug("Got task : {}".format(tid))

        try:
            result = execute_task(req['buffer'])
        except Exception as e:
            result_package = {'task_id': tid,
                              'fabric_id': fabric_id,
                              'exception': serialize_object(e)}
            logger.debug("No result due to exception : {}".format(e))
        else:
            result_package = {'task_id': tid,
                              'fabric_id': fabric_id,
                              'result': serialize_object(result)}
            logger.debug("Result : {}".format(result))

        if snd_req:
            snd_req.Wait()
        snd_req = comm.send(result_package, dest=0, tag=RESULT_TAG)


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


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-i", "--id", required=True,
                        help="Identifier for this fabric")
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

    logger.debug("Python version :{}".format(sys.version))

    fabric_id = str(args.id)
    try:
        if rank == 0:
            master(comm, rank,
                   fabric_id=fabric_id,
                   task_q_url=args.task_url,
                   result_q_url=args.result_url)
        else:
            worker(comm, rank,
                   fabric_id=fabric_id)

    except Exception as e:
        print("Caught error : ", e)
        raise

    print("Done")
