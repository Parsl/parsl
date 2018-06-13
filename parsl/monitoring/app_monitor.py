import time
import logging
from multiprocessing import Process
import psutil
from cmreslogging.handlers import CMRESHandler
import os
from datetime import datetime

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']

run_name = str(datetime.now().minute) + "-" + str(datetime.now().hour) + "-" + str(datetime.now().day)


def monitor(pid):
    host = 'search-parsl-logging-test-2yjkk2wuoxukk2wdpiicl7mcrm.us-east-1.es.amazonaws.com'
    port = 443
    handler = CMRESHandler(hosts=[{'host': host,
                                   'port': port}],
                           use_ssl=True,
                           auth_type=CMRESHandler.AuthType.NO_AUTH,
                           es_index_name="my_python_index",
                           es_additional_fields={'Campaign': "test", 'Username': "yadu"})

    logger = logging.getLogger("ParslElasticsearch")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    logger.info("starting monitoring for {} on {}".format(pid, os.getpid()))
    pm = psutil.Process(pid)
    pm.cpu_percent()
    while True:
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu"] = psutil.cpu_count()
        d["run_id"] = run_name
        for n in ["user","system","children_user","children_system"]:
            d["psutil_process_" + n] = getattr(pm.cpu_times(), n)
        logger.info("test", extra=d)
        time.sleep(2)


def monitor_wrapper(f):
    def wrapped(*args, **kwargs):
        p = Process(target=monitor, args=(os.getpid(),))
        p.start()
        result = f(*args, **kwargs)
        p.terminate()
        return result
    return wrapped


if __name__ == "__main__":
    def f(x):
        for i in range(10**x):
            continue

    wrapped_f = monitor_wrapper(f)
    wrapped_f(9)
