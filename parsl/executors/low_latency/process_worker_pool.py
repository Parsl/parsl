import argparse
import logging

# import zmq
from multiprocessing import Process

from ipyparallel.serialize import unpack_apply_message
from ipyparallel.serialize import serialize_object

from parsl.executors.low_latency import zmq_pipes

logger = logging.getLogger(__name__)


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


def worker(worker_id, task_url, result_url):
    """ TODO: docstring """
    logger.info("Starting worker {}".format(worker_id))

    task_ids_received = []

    task_q = zmq_pipes.TasksIncoming(task_url)
    result_q = zmq_pipes.ResultsOutgoing(result_url)

    while True:
        task_id, buf = task_q.get()
        task_ids_received.append(task_id)

        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(buf, user_ns, copy=False)

        logger.debug("Worker {} received task {}".format(worker_id, task_id))
        result = execute_task(f, args, kwargs, user_ns)
        logger.debug("Worker {} completed task {}".format(worker_id, task_id))

        reply = {"result": result, "worker_id": worker_id}
        result_q.put(task_id, serialize_object(reply))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--workers_per_node", default=1, type=int,
                        help="Number of workers to kick off. Default=1")
    parser.add_argument("-t", "--task_url", required=True,
                        help="REQUIRED: ZMQ url for receiving tasks")
    parser.add_argument("-r", "--result_url", required=True,
                        help="REQUIRED: ZMQ url for posting results")

    args = parser.parse_args()

    workers = []
    for i in range(args.workers_per_node):
        worker = Process(target=worker, 
                         kwargs={"worker_id": i, 
                                 "task_url": args.task_url,
                                 "result_url": args.result_url})
        worker.daemon = True
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
