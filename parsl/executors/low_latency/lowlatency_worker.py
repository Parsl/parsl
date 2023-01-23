#!/usr/bin/env python3

import argparse
import logging
import os
import uuid
# import zmq
from multiprocessing import Process

from parsl.serialize import unpack_apply_message, serialize
from parsl.executors.low_latency import zmq_pipes
from parsl.log_utils import set_file_logger

# __name__ is not reliable, because it could be set to "__main__"
logger = logging.getLogger("parsl.executors.low_latency.lowlatency_worker")


def execute_task(f, args, kwargs, user_ns):
    """
    Deserialize the buffer and execute the task.

    # Returns the result or exception.
    """
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
        exec(code, user_ns, user_ns)

    except Exception as e:
        logger.warning("Caught exception; will raise it: {}".format(e))
        raise e

    else:
        return user_ns.get(resultname)


def worker(worker_id, task_url, debug=True, logdir="workers", uid="1"):
    """ TODO: docstring

    TODO : Cleanup debug, logdir and uid to function correctly
    """

    log_filename = '{}/{}/worker_{}.log'.format(logdir, uid, worker_id)
    try:
        os.makedirs(os.path.dirname(log_filename), 511, True)
    except Exception as e:
        print("Caught exception with trying to make log dirs: {}".format(e))
    set_file_logger(
        filename=log_filename,
        name="parsl.executors.low_latency.lowlatency_worker",
        level=logging.DEBUG if debug is True else logging.INFO,
        format_string="%(asctime)s %(name)s:%(lineno)d Rank:0 [%(levelname)s]  %(message)s",
    )

    logger.info("Starting worker {}".format(worker_id))

    task_ids_received = []

    message_q = zmq_pipes.WorkerMessages(task_url)

    while True:
        print("Worker loop iteration starting")
        task_id, buf = message_q.get()
        task_ids_received.append(task_id)

        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(buf, user_ns, copy=False)

        logger.debug("Worker {} received task {}".format(worker_id, task_id))
        result = execute_task(f, args, kwargs, user_ns)
        logger.debug("Worker {} completed task {}".format(worker_id, task_id))

        reply = {"result": result, "worker_id": worker_id}
        message_q.put(task_id, serialize(reply))
        logger.debug("Result sent")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--workers_per_node", default=1, type=int,
                        help="Number of workers to kick off. Default=1")
    parser.add_argument("-l", "--logdir", default="lowlatency_worker_logs",
                        help="LowLatency worker log directory")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-u", "--uid", default=str(uuid.uuid4()).split('-')[-1],
                        help="Unique identifier string for Manager")

    args = parser.parse_args()

    workers = []
    for i in range(args.workers_per_node):
        worker = Process(target=worker,
                         kwargs={"worker_id": i,
                                 "task_url": args.task_url,
                                 "logdir": args.logdir,
                                 "uid": args.uid
                                 })
        worker.daemon = True
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
