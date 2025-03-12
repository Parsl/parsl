import logging

import parsl
from parsl import python_app
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from parsl.monitoring.monitoring import MonitoringHub


def local_setup():
    threads_config = Config(
        executors=[ThreadPoolExecutor(
            label='threads',
            max_threads=4)
        ],
        monitoring=MonitoringHub(
            hub_address="127.0.0.1",
            logging_level=logging.INFO,
            resource_monitoring_interval=10))

    parsl.load(threads_config)


def local_teardown():
    parsl.clear()


@python_app
def sleeper(dur=25):
    import time
    time.sleep(dur)


@python_app
def cpu_stress(dur=30):
    import time
    s = 0
    start = time.time()
    for i in range(10**8):
        s += i
        if time.time() - start >= dur:
            break
    return s


if __name__ == "__main__":

    tasks = [sleeper() for i in range(8)]
    # tasks = [cpu_stress() for i in range(10)]

    print([i.result() for i in tasks])
