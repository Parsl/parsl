import time
from multiprocessing import Process
import psutil
import os
from parsl.monitoring.db_logger import get_db_logger


def monitor(pid, task_id, db_logger_config, run_id):
    sleep_duration = 10
    if db_logger_config is not None:
        sleep_duration = db_logger_config.get('resource_loop_sleep_duration', sleep_duration)
    time.sleep(sleep_duration)
    simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    summable_values = ['cpu_percent', 'memory_percent', 'num_threads']

    logger = get_db_logger(enable_es_logging=False) if db_logger_config is None\
        else get_db_logger(logger_name=run_id + str(task_id), **{k: v for k, v in db_logger_config.items() if k != 'logger_name'})
    # logger = get_db_logger(enable_es_logging=False)

    # logger.info("starting monitoring for {} on {}".format(pid, os.getpid(), extra={'task_id': task_id, 'task_run_id': run_id}))
    pm = psutil.Process(pid)
    pm.cpu_percent()

    def to_mb(some_bytes):
        return some_bytes / 1024**2

    while True:
        try:
            d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
            d["task_run_id"] = run_id
            d["task_id"] = task_id
            children = pm.children(recursive=True)
            d["psutil_cpu_count"] = psutil.cpu_count()
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
                d['psutil_process_disk_write'] = 0
                d['psutil_process_disk_read'] = 0
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
                    d['psutil_process_disk_write'] += 0
                    d['psutil_process_disk_read'] += 0

            to_be_rounded = ['psutil_process_memory_virtual', 'psutil_process_memory_resident', 'psutil_process_cpu_percent', 'psutil_process_memory_percent',
                             'psutil_process_disk_write', 'psutil_process_disk_read']
            for k in to_be_rounded:
                d[k] = round(d[k], 2)

        finally:
            logger.info("task resource update", extra=d)
            sleep_duration = 10
            if db_logger_config is not None:
                sleep_duration = db_logger_config.get('resource_loop_sleep_duration', sleep_duration)
            time.sleep(sleep_duration)


def monitor_wrapper(f, task_id, db_logger_config, run_id):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, db_logger_config, run_id))
        p.start()
        try:
            return f(*args, **kwargs)
        finally:
            p.terminate()
            p.join()
    return wrapped


if __name__ == "__main__":
    def f(x):
        for i in range(10**x):
            continue

    wrapped_f = monitor_wrapper(f, 0)
    wrapped_f(9)
