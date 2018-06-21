import time
from multiprocessing import Process
import psutil
import os
from parsl.db_logger import get_db_logger

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']


def monitor(pid, task_id, db_logger_config, run_id):
    logger = get_db_logger(enable_es_logging=False) if db_logger_config is None else get_db_logger(**db_logger_config)

    logger.info("starting monitoring for {} on {}".format(pid, os.getpid()))
    pm = psutil.Process(pid)
    pm.cpu_percent()
    while True:
        children = pm.children(recursive=True)
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu"] = psutil.cpu_count()
        d["task_run_id"] = run_id
        d["task_id"] = task_id
        for n in ["user", "system", "children_user", "children_system"]:
            d["psutil_process_" + n] = getattr(pm.cpu_times(), n)
        #for child in children:
        if children[0] is not None:
            c = {"psutil_process_child_" + str(k): v for k, v in children[0].as_dict().items() if k in simple}
            #c = {"psutil_process_child_" + str(k): v if d["psutil_process_child_" + str(k)] is not None else v + d["psutil_process_child_" + str(k)] for k, v in child.as_dict().items() if k in simple}
        d.update(c)
        logger.info("test", extra=d)
        time.sleep(4)


def monitor_wrapper(f, task_id, db_logger_config, run_id):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, db_logger_config, run_id))
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
