import time
import logging
from multiprocessing import Process
import psutil
from cmreslogging.handlers import CMRESHandler

simple = ["cpu_num", 'cpu_percent', 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']


def monitor(p):
    p.start()
    pm = psutil.Process(p.pid)
    pm.cpu_percent()
    while p.is_alive():
        d = {"psutil_process_" + str(k): v for k, v in pm.as_dict().items() if k in simple}
        d["psutil_cpu"] = psutil.cpu_count()
        logger.info("test", extra=d)
        time.sleep(1)
    print("done")


if __name__ == "__main__":
    host = 'search-parsl-logging-test-2yjkk2wuoxukk2wdpiicl7mcrm.us-east-1.es.amazonaws.com'
    port = 443
    index_name = "parsl.campaign"

    handler = CMRESHandler(hosts=[{'host': host,
                                   'port': port}],
                           use_ssl=True,
                           auth_type=CMRESHandler.AuthType.NO_AUTH,
                           es_index_name="my_python_index",
                           es_additional_fields={'Campaign': "test", 'Username': "yadu"})

    logger = logging.getLogger("ParslElasticsearch")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    def f(x):
        for i in range(10**x):
            continue

    t = Process(target=f, args=(9,))
    monitor(t)
