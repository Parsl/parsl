#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import threading
import time
import uuid
import zmq

from mpi4py import MPI

from ipyparallel.serialize import unpack_apply_message  # pack_apply_message,
from ipyparallel.serialize import serialize_object
from parsl.executors.mpix import zmq_pipes

RESULT_TAG = 10
TASK_REQUEST_TAG = 11



def recv_result(comm, result_q):
        info = MPI.Status()
        logger.debug("[RESULT_Q MANAGER] before comm.recv}")
        result = comm.recv(source=MPI.ANY_SOURCE, tag=RESULT_TAG, status=info)
        logger.debug("[RESULT_Q MANAGER] after comm.recv")
        logger.debug("[RESULT_Q MANAGER] Received result : {} - putting in result_q".format(result))
        result_q.put(result)
        logger.debug("[RESULT_Q MANAGER] after result_q put")

def recv_task_request(comm, ready_worker_queue):
 
    info = MPI.Status()
    logger.info("calling comm.recv")
    req = comm.recv(source=MPI.ANY_SOURCE, tag=TASK_REQUEST_TAG, status=info)
    logger.info("returned from comm.recv")

    worker_rank = info.Get_source()

    ready_worker_queue.append(worker_rank)
       

def master(comm, rank, task_q_url=None, result_q_url=None):
    """ Due to the asynchronous nature of the the task queue and results queue
    we have the main thread processing requests for jobs and the a secondary thread
    that exclusively listens for results using tags
    """

    logger.info("Master started")

    master_id = str(uuid.uuid4())
    task_queue = zmq_pipes.JobsQIncoming(task_q_url, server_id=master_id)
    result_queue = zmq_pipes.ResultsQOutgoing(result_q_url, server_id=master_id)

    ready_worker_queue = []
    ready_task_queue = []

    logger.info("Connected to task_queue:{}".format(task_queue))
    logger.info("Connected to result_queue:{}".format(result_queue))

    # Sync everything
    comm.Barrier()
    logger.debug("Master synced with workers")

    count = 1
    start = time.time()
    abort_flag = False

    while not abort_flag:

#        logger.info("Start loop")
#        logger.info("Ready worker queue length: {}".format(len(ready_worker_queue)))
#        logger.info("Ready task queue length: {}".format(len(ready_task_queue)))

        # probe if there is a result:

        info = MPI.Status()

        if comm.Iprobe(status=info):
            logger.info("There is a message waiting in MPI")
            tag = info.Get_tag()
            logger.info("Message has tag {}".format(tag))

            if tag == RESULT_TAG:
                recv_result(comm, result_queue)

            elif tag == TASK_REQUEST_TAG:
                recv_task_request(comm, ready_worker_queue)

            else:
                logger.error("Unknown tag {} - ignoring this message and continuing".format(tag))


        try:
            task = task_queue.get(timeout=1) # this is a ratelimit on progress of the whole loop - maybe should be smaller / completely nonblocking for fast apps
            if task["task_id"] == "STOP":
                logger.info("Received STOP request, preparing to terminate MPI fabric")
                abort_flag = True
                break
            else:
                ready_task_queue.append(task)

        except zmq.Again:
             pass
#            logger.info("zmq task queue has no task on this iteration")


        if(len(ready_task_queue) > 0 and len(ready_worker_queue) > 0):
            task = ready_task_queue.pop()
            worker_rank = ready_worker_queue.pop()
            comm.send(task, dest=worker_rank, tag=worker_rank)
            count += 1

        else:
            pass
#            logger.info("Nothing to match between ready worker and task queue in this iteration")

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


def worker(comm, rank):
    logger.info("Worker started")

    # Sync worker with master
    comm.Barrier()
    logger.debug("Synced")

    task_request = b'TREQ'

    while True:
        comm.send(task_request, dest=0, tag=TASK_REQUEST_TAG)
        req = comm.recv(source=0, tag=rank)

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
        comm.send(result_package, dest=0, tag=RESULT_TAG)


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

    try:
        if rank == 0:
            master(comm, rank,
                   task_q_url=args.task_url,
                   result_q_url=args.result_url)
        else:
            worker(comm, rank)
    except Exception as e:
        print("Caught error : ", e)
        raise

    print("Done")
