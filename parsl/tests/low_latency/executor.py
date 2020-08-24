import argparse
import time
import logging
from statistics import mean, stdev

import zmq
from multiprocessing import Process, Manager

from parsl.serialize import ParslSerializer
parsl_serializer = ParslSerializer()
pack_apply_message = parsl_serializer.pack_apply_message
unpack_apply_message = parsl_serializer.unpack_apply_message
deserialize_object = parsl_serializer.deserialize

from constants import CLIENT_IP_FILE
from parsl.addresses import address_by_interface
from worker import execute_task

logger = logging.getLogger(__name__)


def simple_executor(f_all, args_all, kwargs_all, num_tasks):
    serialization_times = []
    exec_times = []
    results = []

    for i in range(num_tasks):
        task_id = i
        start_time = time.time()
        buf = pack_apply_message(f=next(f_all), args=next(args_all),
                                 kwargs=next(kwargs_all),
                                 buffer_threshold=1024 * 1024)
        serialization_times.append(time.time() - start_time)

        start_time = time.time()
        user_ns = locals()
        user_ns.update({'__builtins__': __builtins__})
        f, args, kwargs = unpack_apply_message(buf, user_ns, copy=False)
        result = execute_task(f, args, kwargs, user_ns)
        exec_times.append(time.time() - start_time)

        results.append(result)

    avg_serialization_time = sum(
        serialization_times) / len(serialization_times) * 10 ** 6
    avg_execution_time = sum(exec_times) / len(exec_times) * 10 ** 6

    return {
        "avg_serialization_time": avg_serialization_time,
        "avg_execution_time": avg_execution_time,
        "results": results
    }


def dealer_executor(f_all, args_all, kwargs_all, num_tasks, return_dict,
                    port=5559, interchange=True, warmup=10):
    label = "DEALER-INTERCHANGE-REP" if interchange else "DEALER-REP"
    logger.info("Starting executor:{}".format(label))

    serialization_times = []
    deserialization_times = []
    send_times = {}
    exec_times = {}
    results = []

    context = zmq.Context()
    dealer = context.socket(zmq.DEALER)
    dealer.bind("tcp://*:{}".format(port))

    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)

    num_send = 0
    num_recv = 0

    while True:
        socks = dict(poller.poll(1))
        if num_send < num_tasks:
            task_id = num_send
            task_id_bytes = task_id.to_bytes(4, "little")
            start_time = time.time()
            buf = pack_apply_message(f=next(f_all), args=next(args_all),
                                     kwargs=next(kwargs_all),
                                     buffer_threshold=1024 * 1024)
            serialization_times.append(time.time() - start_time)

            logger.debug("Manager sending task {}".format(task_id))
            send_times[task_id] = time.time()
            dealer.send_multipart([b"", task_id_bytes] + buf)
            num_send += 1

        if dealer in socks and socks[dealer] == zmq.POLLIN:
            buf = dealer.recv_multipart()
            recv_time = time.time()

            start_time = time.time()
            msg = deserialize_object(buf[2:])[0]
            deserialization_times.append(time.time() - start_time)

            logger.debug("Got message {}".format(msg))
            task_id = int.from_bytes(buf[1], "little")
            results.append(msg["result"])

            if num_recv >= warmup:
                # Ignore the first `warmup` tasks
                exec_times[task_id] = recv_time - send_times[task_id]

            num_recv += 1
            logger.debug("Dealer received result {}".format(task_id))
            if num_recv == num_tasks:
                break

    avg_serialization_time = sum(
        serialization_times) / len(serialization_times) * 10 ** 6
    avg_execution_time = sum(exec_times.values()) / len(exec_times) * 10 ** 6

    return_dict["avg_serialization_time"] = avg_serialization_time
    return_dict["avg_execution_time"] = avg_execution_time
    return_dict["results"] = results


def double(x):
    return 2 * x


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-tasks", default=10000, type=int,
                        help="Number of tasks to send for benchmark")
    parser.add_argument("--num-trials", default=10, type=int,
                        help="Number of trials to run for benchmarking")
    parser.add_argument("--warmup", default=100, type=int,
                        help="Number of warmup runs before benchmarking")
    parser.add_argument("--interchange", action="store_true", default=False,
                        help="Whether an interchange is being used")
    parser.add_argument("--client-port", default=5560, type=int,
                        help="Port for client to communicate on")
    parser.add_argument("--localhost", default=False, action="store_true",
                        help="True if communication is on localhost")
    args = parser.parse_args()

    # Write IP address to file so that workers can access it
    if not args.localhost:
        ip = address_by_interface("eth0")
        with open(CLIENT_IP_FILE, "w") as fh:
            fh.write(ip)
        print("Wrote IP address {} to file {}".format(ip, CLIENT_IP_FILE))

    # Parameters for worker requests
    def f_all():
        return (double for _ in range(args.num_tasks))

    def args_all():
        return ([i] for i in range(args.num_tasks))

    def kwargs_all():
        return ({} for _ in range(args.num_tasks))

    serialization_times = []
    execution_times = []

    # Every trial sends the same jobs again and benchmarks them
    for _ in range(args.num_trials):
        m = Manager()
        return_dict = m.dict()
        manager = Process(target=dealer_executor,
                          kwargs={"f_all": f_all(), "args_all": args_all(),
                                  "kwargs_all": kwargs_all(),
                                  "num_tasks": args.num_tasks,
                                  "port": args.client_port,
                                  "interchange": args.interchange,
                                  "warmup": args.warmup,
                                  "return_dict": return_dict})
        manager.start()
        manager.join()

        serialization_times.append(return_dict["avg_serialization_time"])
        execution_times.append(return_dict["avg_execution_time"])

    # Print stats
    label = "[DEALER-INTERCHANGE-REP]" if args.interchange else "[DEALER-REP]"
    s = stdev(serialization_times) if len(serialization_times) > 1 else 0
    print("{} Avg Serialization Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(label, mean(serialization_times), s))
    s = stdev(execution_times) if len(execution_times) > 1 else 0
    print("{} Avg Execution Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(label, mean(execution_times), s))
