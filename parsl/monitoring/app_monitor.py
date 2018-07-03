import time
from multiprocessing import Process
import psutil
import os
from parsl.db_logger import get_db_logger

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
summable_values = ['cpu_percent', 'memory_percent', 'num_threads']


def monitor(pid, task_id, db_logger_config, run_id):

    try:
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
                # this may be the wrong approach as it could give false security of low disk usage
                # d['psutil_process_disk_write'] = 0
                # d['psutil_process_disk_read'] = 0
                print("psutil disk access denied exception")
                d['psutil_process_disk_write'] = -1
                d['psutil_process_disk_read'] = -1
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
                    print("psutil disk access denied exception")
            logger.info("test", extra=d)
            time.sleep(5)
    except Exception:
        print('Exception in the monitoring')
    else:
        print('No exception in the monitoring loop for ' + str(task_id))


def monitor_wrapper(f, task_id, db_logger_config, run_id):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(), task_id, db_logger_config, run_id))
        p.start()
        try:
            result = f(*args, **kwargs)
        except:
            print("App Failure terminating monitoring process")
        finally:
            p.terminate()
        return result
    return wrapped


if __name__ == "__main__":
    def f(x):
        for i in range(10**x):
            continue

    wrapped_f = monitor_wrapper(f, 0)
    wrapped_f(9)
