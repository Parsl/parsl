import time
from multiprocessing import Process
import psutil
import os
from parsl.monitoring.db_logger import get_db_logger


def monitor(pid, task_id, db_logger_config, run_id):
    simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    summable_values = ['cpu_percent', 'memory_percent', 'num_threads']

    logger = get_db_logger(enable_es_logging=False) if db_logger_config is None else get_db_logger(**db_logger_config)
    logger.info("starting monitoring for {} on {}".format(pid, os.getpid(), extra={'task_id': task_id, 'task_run_id': run_id}))
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
        d['psutil_process_time_user'] = pm.cpu_times().user
        d['psutil_process_time_system'] = pm.cpu_times().system
        d['psutil_process_children_count'] = len(children)
        try:
            d['psutil_process_disk_write'] = to_mb(pm.io_counters().write_bytes)
            d['psutil_process_disk_read'] = to_mb(pm.io_counters().read_bytes)
        except psutil._exceptions.AccessDenied:
            # not setting should result in a null value that should report as a blank and not "spoil" the kibana aggregations
            pass
        for child in children:
            for k, v in child.as_dict(attrs=summable_values).items():
                d['psutil_process_' + str(k)] += v
            d['psutil_process_time_user'] += child.cpu_times().user
            d['psutil_process_time_system'] += child.cpu_times().system
            d['psutil_process_memory_virtual'] += to_mb(child.memory_info().vms)
            d['psutil_process_memory_resident'] += to_mb(child.memory_info().rss)
            try:
                d['psutil_process_disk_write'] += to_mb(child.io_counters().write_bytes)
                d['psutil_process_disk_read'] += to_mb(child.io_counters().read_bytes)
            except psutil._exceptions.AccessDenied:
                pass
        logger.info("test", extra=d)
        time.sleep(5)


def monitor_wrapper(f, task_id, db_logger_config, run_id):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, db_logger_config, run_id))
        p.start()
        try:
            return f(*args, **kwargs)
        finally:
            p.terminate()
    return wrapped
