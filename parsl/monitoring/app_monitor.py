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

    def to_mb(some_bytes):
        return some_bytes / 1024**2

    while True:
        children = pm.children(recursive=True)
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu_count"] = psutil.cpu_count()
        d["task_run_id"] = run_id
        d["task_id"] = task_id
        d['psutil_process_memory_virtual'] = to_mb(pm.memory_info().vms)
        d['psutil_process_memory_resident'] = to_mb(pm.memory_info().rss)
        for n in ["user", "system", "children_user", "children_system"]:
            d["psutil_process_" + n] = getattr(pm.cpu_times(), n)
        for child in children:
            try:
                c = {"psutil_process_child_" + str(k): v for k, v in child.as_dict().items() if (k in simple and v > d.get("psutil_process_child_" + str(k), 0))}
            except TypeError:
            # ignore an error that occurs if compare the comparison is comparing against a bad or non existant value by simply replacing it with the newer child's info
                c = {"psutil_process_child_" + str(k): v for k, v in child.as_dict().items() if k in simple}
            c['psutil_process_child_memory_virtual'] = to_mb(child.memory_info().vms)
            c['psutil_process_child_memory_resident'] = to_mb(child.memory_info().rss)
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
