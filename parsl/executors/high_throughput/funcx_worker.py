#!/usr/bin/env python3

import logging
import argparse
import zmq
import os
import sys
import pickle
from ipyparallel.serialize import serialize_object, unpack_apply_message

from parsl.app.errors import RemoteExceptionWrapper


def funcx_worker(worker_id, pool_id, task_url, reg_url, no_reuse, logdir, debug=False):
    """
    Funcx worker will use the REP sockets to:
         task = recv ()
         result = execute(task)
         send(result)
    """

    #print("Exiting worker. Container will not be reused")
    #print("Exited with code: {}".format(exit()))



    try:
        os.makedirs("{}/{}".format(logdir, pool_id))
    except Exception:
        pass

    start_file_logger('{}/{}/funcx_worker_{}.log'.format(logdir, pool_id, worker_id),
                      worker_id,
                      name="worker_log",
                      level=logging.DEBUG if debug else logging.INFO)

    # Sync worker with master
    logger.info('Worker {} started'.format(worker_id))
    if debug:
        logger.debug("Debug logging enabled")

    context = zmq.Context()

    registration_socket = context.socket(zmq.REQ)
    registration_socket.RCVTIMEO=1000
    registration_socket.connect(reg_url)
    logger.info("Connecting to {} for registration".format(reg_url))
    registration_socket.send_pyobj(worker_id)
    try:
        msg = registration_socket.recv_pyobj()
    except zmq.Again:
        logger.critical("Registered with manager failed")
    else:
        logger.info("Registered with manager successful")


    funcx_worker_socket = context.socket(zmq.REP)
    funcx_worker_socket.connect(task_url)
    logger.info("Connecting to {}".format(task_url))
    logger.info("Container will exit after single task: {}".format(no_reuse))

    while True:
        # This task receiver socket is blocking.
        try:
            b_task_id, *buf = funcx_worker_socket.recv_multipart()
        except Exception as e:
            logger.debug(e)
        # msg = task_socket.recv_pyobj()
        logger.debug("Got buffer : {}".format(buf))

        task_id = int.from_bytes(b_task_id, "little")
        logger.info("Received task {}".format(task_id))

        try:
            result = execute_task(buf)
            serialized_result = serialize_object(result)

        except Exception:
            result_package = {'task_id': task_id, 'exception': serialize_object(RemoteExceptionWrapper(*sys.exc_info()))}
            logger.debug("Got exception something")
        else:
            result_package = {'task_id': task_id, 'result': serialized_result}

        logger.info("Completed task {}".format(task_id))
        pkl_package = pickle.dumps(result_package)

        funcx_worker_socket.send_multipart([pkl_package])

        if no_reuse:
            logger.info("Exiting worker. Container will not be reused, breaking...")
            funcx_worker_socket.close()
            context.term()
            return None


def execute_task(bufs):
    """Deserialize the buffer and execute the task.

    Returns the result or throws exception.
    """

    logger.debug("Inside execute_task function")
    user_ns = locals()
    user_ns.update({'__builtins__': __builtins__})

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    logger.debug("Message unpacked")

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

    logger.debug("Namespace updated")

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:
        exec(code, user_ns, user_ns)

    except Exception as e:
        raise e

    else:
        return user_ns.get(resultname)


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


if __name__ == "__main__":
    # no_reuse = False
    # open('my_file.txt', 'a').close()
    # exit()
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker_id", help="ID of worker from process_worker_pool", required=True)
    parser.add_argument("--pool_id", help="ID of our process_worker_pool", required=True)
    parser.add_argument("--task_url", help="URL from which we receive tasks and send replies", required=True)
    parser.add_argument("--reg_url", help="URL for registering the worker", required=True)
    parser.add_argument("--logdir", help="Directory path where worker log files written", required=True)
    parser.add_argument("--no_reuse", help="If exists, run in no_reuse mode on containers", action="store_true", required=False)

    args = parser.parse_args()
    worker = funcx_worker(args.worker_id, args.pool_id, args.task_url, args.reg_url, args.no_reuse, args.logdir)
    
