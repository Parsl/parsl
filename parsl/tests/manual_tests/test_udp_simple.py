import parsl
from parsl import *
# from parsl.monitoring.db_logger import MonitoringConfig
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
import logging

parsl.set_stream_logger()

threads_config = Config(
    executors=[ThreadPoolExecutor(
        label='threads',
        max_threads=4)
    ],
    monitoring=MonitoringHub(
        hub_address="127.0.0.1",
        hub_port=55055,
        logging_level=logging.DEBUG
    )
)

dfk = DataFlowKernel(config=threads_config)


@App('python', dfk)
def sleeper(dur=5):
    import time
    time.sleep(dur)


@App('python', dfk)
def cpu_stress(workers=1, timeout=10, inputs=[], outputs=[]):
    s = 0
    for i in range(10**8):
        s += i
    return s


if __name__ == "__main__":

    sleepers = [sleeper(i) for i in range(5)]

    print([i.result() for i in sleepers])
