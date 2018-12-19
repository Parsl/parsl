import argparse
from statistics import mean, stdev

from multiprocessing import Process, Manager
from executor import dealer_executor, simple_executor
from interchange import dealer_interchange
from worker import dealer_worker  # , execute_task


def double(x):
    return 2 * x


def benchmark_simple_executor(f_all, args_all, kwargs_all, num_tasks,
                              num_trials=10):
    serialization_times = []
    execution_times = []

    for _ in range(num_trials):
        stats = simple_executor(f_all(), args_all(), kwargs_all(), num_tasks)
        serialization_times.append(stats["avg_serialization_time"])
        execution_times.append(stats["avg_execution_time"])

    s = stdev(serialization_times) if len(serialization_times) > 1 else 0
    print("[WITHOUT-ZEROMQ] Avg Serialization Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(mean(serialization_times), s))
    s = stdev(execution_times) if len(execution_times) > 1 else 0
    print("[WITHOUT-ZEROMQ] Avg Execution Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(mean(execution_times), s))


def benchmark_dealer_rep(f_all, args_all, kwargs_all, num_tasks, num_workers,
                         interchange=False, num_trials=10, warmup=100):
    serialization_times = []
    execution_times = []
    manager_port = 5559 if interchange else 5560
    worker_port = 5560

    for _ in range(num_trials):
        m = Manager()
        return_dict = m.dict()
        manager = Process(target=dealer_executor,
                          kwargs={"f_all": f_all(), "args_all": args_all(),
                                  "kwargs_all": kwargs_all(),
                                  "num_tasks": num_tasks, "port": manager_port,
                                  "interchange": interchange, "warmup": warmup,
                                  "return_dict": return_dict})
        manager.start()
        if interchange:
            interchange = Process(target=dealer_interchange,
                                  kwargs={"manager_port": manager_port,
                                          "worker_port": worker_port})
            interchange.daemon = True
            interchange.start()
        workers = []
        for i in range(num_workers):
            worker = Process(target=dealer_worker,
                             kwargs={"worker_id": i, "port": worker_port})
            worker.daemon = True
            worker.start()
            workers.append(worker)

        manager.join()
        if interchange:
            interchange.terminate()
        for worker in workers:
            worker.terminate()

        serialization_times.append(return_dict["avg_serialization_time"])
        execution_times.append(return_dict["avg_execution_time"])

    label = "[DEALER-INTERCHANGE-REP]" if interchange else "[DEALER-REP]"
    s = stdev(serialization_times) if len(serialization_times) > 1 else 0
    print("{} Avg Serialization Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(label, mean(serialization_times), s))
    s = stdev(execution_times) if len(execution_times) > 1 else 0
    print("{} Avg Execution Time\n"
          "Mean = {:=10.4f} us, Stdev = {:=10.4f} us"
          .format(label, mean(execution_times), s))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-tasks", default=10000, type=int,
                        help="Number of tasks to send for benchmark")
    parser.add_argument("--num-workers", default=1, type=int,
                        help="Number of workers to use for Dealer-Rep")
    parser.add_argument("--num-trials", default=10, type=int,
                        help="Number of trials to run for benchmarking")
    args = parser.parse_args()

    def f_all():
        return (double for _ in range(args.num_tasks))

    def args_all():
        return ([i] for i in range(args.num_tasks))

    def kwargs_all():
        return ({} for _ in range(args.num_tasks))

    # Naive Vanilla Version
    benchmark_simple_executor(f_all, args_all, kwargs_all, args.num_tasks,
                              num_trials=args.num_trials)

    # Using Dealer-Reply without Interchange
    benchmark_dealer_rep(f_all, args_all, kwargs_all, args.num_tasks,
                         args.num_workers, num_trials=args.num_trials,
                         interchange=False)

    # Using Dealer-Reply with Interchange
    benchmark_dealer_rep(f_all, args_all, kwargs_all, args.num_tasks,
                         args.num_workers, num_trials=args.num_trials,
                         interchange=True)
