import time
import logging
from multiprocessing import Process
import psutil
from cmreslogging.handlers import CMRESHandler

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']


def monitor(p):
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

    p.start()
    pm = psutil.Process(p.pid)
    pm.cpu_percent()
    while p.is_alive():
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu"] = psutil.cpu_count()
        logger.info("test", extra=d)
        time.sleep(1)
    return p.result()


def monitor_wrapper(f):
    def wrapped(*args, **kwargs):
        p = Process(target=f, args=args, kwargs=kwargs)
        return monitor(p)
    return wrapped


if __name__ == "__main__":
    def f(x):
        for i in range(10**x):
            continue

    wrapped_f = monitor_wrapper(f)
    wrapped_f(9)
