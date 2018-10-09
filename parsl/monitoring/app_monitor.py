import time
from multiprocessing import Process
import os
from parsl.monitoring.db_logger import get_db_logger


def monitor(pid, task_id, monitoring_config, run_id):
    """Internal
    Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.
    """
    import psutil

    if monitoring_config is None:
        logger = get_db_logger()
    else:
        logger = get_db_logger(logger_name=run_id + str(task_id), monitoring_config=monitoring_config)

    sleep_duration = monitoring_config.resource_loop_sleep_duration
    time.sleep(sleep_duration)
    # these values are simple to log. Other information is available in special formats such as memory below.
    simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
    # values that can be summed up to see total resources used by task process and its children
    summable_values = ['cpu_percent', 'memory_percent', 'num_threads']

    pm = psutil.Process(pid)
    pm.cpu_percent()

    while True:
        try:
            d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
            d["run_id"] = run_id
            d["task_id"] = task_id
            children = pm.children(recursive=True)
            d["psutil_cpu_count"] = psutil.cpu_count()
            d['psutil_process_memory_virtual'] = pm.memory_info().vms
            d['psutil_process_memory_resident'] = pm.memory_info().rss
            d['psutil_process_time_user'] = pm.cpu_times().user
            d['psutil_process_time_system'] = pm.cpu_times().system
            d['psutil_process_children_count'] = len(children)
            try:
                d['psutil_process_disk_write'] = pm.io_counters().write_bytes
                d['psutil_process_disk_read'] = pm.io_counters().read_bytes
            except psutil._exceptions.AccessDenied:
                # occassionally pid temp files that hold this information are unvailable to be read so set to zero
                d['psutil_process_disk_write'] = 0
                d['psutil_process_disk_read'] = 0
            for child in children:
                for k, v in child.as_dict(attrs=summable_values).items():
                    d['psutil_process_' + str(k)] += v
                d['psutil_process_time_user'] += child.cpu_times().user
                d['psutil_process_time_system'] += child.cpu_times().system
                d['psutil_process_memory_virtual'] += child.memory_info().vms
                d['psutil_process_memory_resident'] += child.memory_info().rss
                try:
                    d['psutil_process_disk_write'] += child.io_counters().write_bytes
                    d['psutil_process_disk_read'] += child.io_counters().read_bytes
                except psutil._exceptions.AccessDenied:
                    # occassionally pid temp files that hold this information are unvailable to be read so add zero
                    d['psutil_process_disk_write'] += 0
                    d['psutil_process_disk_read'] += 0

        finally:
            logger.info("task resource update", extra=d)
            sleep_duration = monitoring_config.resource_loop_sleep_duration
            time.sleep(sleep_duration)


def monitor_wrapper(f, task_id, monitoring_config, run_id):
    """ Internal
    Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
    """
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, monitoring_config, run_id))
        p.start()
        try:
            return f(*args, **kwargs)
        finally:
            # Note: I believe finally blocks are not called if the python parsl process is killed with certain SIGs which may leave the monitor as a zombie
            p.terminate()
            p.join()
    return wrapped
