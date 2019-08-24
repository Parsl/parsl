import parsl
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import AprunLauncher
from parsl.providers import CobaltProvider
from parsl.addresses import address_by_hostname

import logging
from parsl.app.app import python_app


def local_setup():
    threads_config = Config(
        executors=[
            HighThroughputExecutor(
                label="theta_htex",
                # worker_debug=True,
                cores_per_worker=4,
                address=address_by_hostname(),
                provider=CobaltProvider(
                    queue='debug-flat-quad',
                    account='CSC249ADCD01',
                    launcher=AprunLauncher(overrides="-d 64"),
                    worker_init='source activate parsl-issues',
                    init_blocks=1,
                    max_blocks=1,
                    min_blocks=1,
                    nodes_per_block=4,
                    cmd_timeout=60,
                    walltime='00:10:00',
                ),
            )
        ],
        monitoring=MonitoringHub(
            hub_address=address_by_hostname(),
            hub_port=55055,
            logging_level=logging.DEBUG,
            resource_monitoring_interval=10),
        strategy=None)
    parsl.load(threads_config)


def local_teardown():
    parsl.clear()


@python_app
def inc(x):
    import time
    start = time.time()
    sleep_duration = 30.0
    while True:
        x += 1
        end = time.time()
        if end - start >= sleep_duration:
            break
    return x


@python_app
def add_inc(inputs=[]):
    import time
    start = time.time()
    sleep_duration = 30.0
    res = sum(inputs)
    while True:
        res += 1
        end = time.time()
        if end - start >= sleep_duration:
            break
    return res


if __name__ == "__main__":

    total = 200
    half = int(total / 2)
    one_third = int(total / 3)
    two_third = int(total / 3 * 2)
    futures_1 = [inc(i) for i in range(total)]
    futures_2 = [add_inc(inputs=futures_1[0:half]),
                 add_inc(inputs=futures_1[half:total])]
    futures_3 = [inc(futures_2[0]) for _ in range(half)] + [inc(futures_2[1]) for _ in range(half)]
    futures_4 = [add_inc(inputs=futures_3[0:one_third]),
                 add_inc(inputs=futures_3[one_third:two_third]),
                 add_inc(inputs=futures_3[two_third:total])]

    print([f.result() for f in futures_4])
    print("Done")
