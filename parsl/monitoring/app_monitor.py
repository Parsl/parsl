import time
from multiprocessing import Process
import psutil
import os
from datetime import datetime
from parsl.db_logger import get_db_logger

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']

run_name = str(datetime.now().minute) + "-" + str(datetime.now().hour) + "-" + str(datetime.now().day)


def monitor(pid, task_id, db_logger_config):
    logger = get_db_logger(enable_es_logging=False) if db_logger_config is None else get_db_logger(**db_logger_config)

    logger.info("starting monitoring for {} on {}".format(pid, os.getpid()))
    pm = psutil.Process(pid)
    pm.cpu_percent()
    while True:
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu"] = psutil.cpu_count()
        d["run_id"] = run_name
        d["task_id"] = task_id
        for n in ["user", "system", "children_user", "children_system"]:
            d["psutil_process_" + n] = getattr(pm.cpu_times(), n)
        logger.info("test", extra=d)
        time.sleep(2)


def monitor_wrapper(f, task_id, db_logger_config):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, db_logger_config))
        p.start()
        result = f(*args, **kwargs)
        p.terminate()
        return result
    return wrapped


if __name__ == "__main__":
    def f(x):
        for i in range(10**x):
            continue

    wrapped_f = monitor_wrapper(f, 0)
    wrapped_f(9)
