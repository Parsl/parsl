import argparse
import logging

import zmq
from multiprocessing import Process
from ipyparallel.serialize import unpack_apply_message
from ipyparallel.serialize import serialize_object

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

from constants import CLIENT_IP_FILE, INTERCHANGE_IP_FILE
from utils import ping_time


def execute_task(f, args, kwargs, user_ns):
    """
    Deserialize the buffer and execute the task.

    Returns the result or exception.
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


def dealer_worker(worker_id, ip="localhost", port=5560):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect("tcp://{}:{}".format(ip, port))
    print("Starting worker {}".format(worker_id))

    task_ids_received = []

    while True:
        bufs = socket.recv_multipart()
        task_id = int.from_bytes(bufs[0], "little")
        task_ids_received.append(task_id)

        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(bufs[1:], user_ns, copy=False)

        logger.debug("Worker {} received task {}".format(worker_id, task_id))
        result = execute_task(f, args, kwargs, user_ns)
        logger.debug("Worker result: {}".format(result))
        reply = {"result": result, "worker_id": worker_id}
        socket.send_multipart([bufs[0]] + serialize_object(reply))

        print("Worker {} received {} tasks"
              .format(worker_id, len(task_ids_received)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", default=1, type=int,
                        help="Number of workers to use for Dealer-Rep")
    parser.add_argument("--localhost", default=False, action="store_true",
                        help="True if communication is on localhost")
    parser.add_argument("--worker-port", default=5559, type=int,
                        help="Port for workers to communicate on")
    parser.add_argument("--interchange", action="store_true", default=False,
                        help="Whether an interchange is being used")
    args = parser.parse_args()

    # Read in IP address to communicate to
    if not args.localhost:
        ip_file = INTERCHANGE_IP_FILE if args.interchange else CLIENT_IP_FILE
        with open(ip_file, "r") as fh:
            ip = fh.read().strip()
        print("Read IP {} from file {}".format(ip, ip_file))
        print("Ping time to IP {}: {} us".format(ip, ping_time(ip)))
    else:
        ip = "localhost"

    workers = []
    for i in range(args.num_workers):
        worker = Process(target=dealer_worker,
                         kwargs={"worker_id": i, "ip": ip,
                                 "port": args.worker_port})
        worker.daemon = True
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
