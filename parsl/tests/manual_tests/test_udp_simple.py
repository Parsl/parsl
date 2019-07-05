from parsl import *
# from parsl.monitoring.db_logger import MonitoringConfig
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
import logging

# parsl.set_stream_logger()

threads_config = Config(
    executors=[ThreadPoolExecutor(
        label='threads',
        max_threads=4)
    ],
    monitoring=MonitoringHub(
        hub_address="127.0.0.1",
        hub_port=55055,
        logging_level=logging.INFO,
        resource_monitoring_interval=10,
    )
)

dfk = DataFlowKernel(config=threads_config)


@App('python', dfk)
def sleeper(dur=25):
    import time
    time.sleep(dur)


@App('python', dfk)
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
