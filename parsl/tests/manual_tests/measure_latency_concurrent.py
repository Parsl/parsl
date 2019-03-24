import time
import argparse
import logging

import parsl


def sleep(seconds):
    import time
    time.sleep(seconds)


def double(x):
    return x * 2


def measure_latency_with_task_bound(num_tasks, executor, max_pending,
                                    primers=100):
    """This measures task latencies while ensuring that at any given time,
    at most `max_pending` tasks are pending.

    :param num_tasks: Number of tasks to test with
    :param executor: The executor to use
    :param max_pending: Maximum pending tasks at a given time
    :param primers: Number of tasks used for priming (default: {10})
    """
    print("Priming with {} tasks".format(primers))
    start = time.time()
    for i in range(primers):
        executor.submit(double, i).result()
    delta = time.time() - start
    print("Priming done in {:10.4f} s".format(delta))

    print("Launching {} tasks with at most {} pending at a time"
          .format(num_tasks, max_pending))

    start_all = time.time()
    times = []
    num_pending = 0
    num_sent = 0
    num_received = 0
    start = {}
    futures = {}

    while num_received < num_tasks:
        # Send out as many tasks as possible
        batch_size = 0
        while num_sent < num_tasks and num_pending < max_pending:
            futures[num_sent] = executor.submit(double, num_sent)
            start[num_sent] = time.time()
            # print("Sent task with id {}".format(num_sent))
            num_sent += 1
            num_pending += 1
            batch_size += 1

        # Option 1 for getting results
        for i in range(num_sent - batch_size, num_sent):
            futures[i].result()
            delta = time.time() - start[i]
            times.append(delta * 1000)
            del futures[i]
            num_received += 1
            num_pending -= 1

        # Option 2 for getting results
        # # Receive all results that are done
        # to_delete = set()
        # for i, fu in futures.items():
        #     if fu.done():
        #         fu.result()
        #         delta = time.time() - start[i]
        #         times.append(delta * 1000)
        #         print("Got result with id {}".format(i))
        #         to_delete.add(i)
        #         num_received += 1
        #         num_pending -= 1
        # for i in to_delete:
        #     del futures[i]

    delta_all = time.time() - start_all
    from math import ceil
    print("All latencies: {}".format([ceil(t) for t in sorted(times)]))

    print("Time to complete {} tasks: {:8.3f} s".format(num_tasks, delta_all))
    print("Latency avg:{:8.3f}ms  min:{:8.3f}ms  max:{:8.3f}ms".format(
            sum(times) / len(times), min(times), max(times)))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--count", default=1000,
                        help="Count of apps to launch", type=int)
    parser.add_argument("-b", "--num-blocks", default=1,
                        help="Number of blocks to kick off", type=int)
    parser.add_argument("-n", "--workers-per-node", default=4,
                        help="Number of workers per node", type=int)
    parser.add_argument("-w", "--warmup", default=100,
                        help="Number of warmup tasks", type=int)
    # parser.add_argument("-d", "--debug", action='store_true',
                        # help="Whether to print debug messages")

    args = parser.parse_args()
    parsl.set_stream_logger(level=logging.INFO)

    from llex_local import llex_config
    config = llex_config(args.workers_per_node, args.num_blocks)
    # dfk = parsl.load(config)
    dfk = parsl.load(config)
    executor = dfk.executors["llex_local"]
    total_workers = args.workers_per_node * args.num_blocks

    measure_latency_with_task_bound(args.count, executor, total_workers,
                                    args.warmup)
