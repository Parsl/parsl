import parsl
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import FuncXExecutor
from parsl.configs.htex_local import config

import logging
import sys, time
from parsl.app.app import python_app
from funcx.sdk.client import FuncXClient


def local_setup():
    fx_config = Config(
        executors=[
            FuncXExecutor(
                label="funcX",
                # worker_debug=True,
                endpoints=['870b1d5d-28b0-4962-877f-886d96d4d785'],
            )
        ],
    )
    parsl.load(fx_config)


def local_setup_htex():
    parsl.load(config)


def local_teardown():
    parsl.clear()


@python_app
def inc(x):
    import time
    start = time.time()
    sleep_duration = 0
    x = 0
    while True:
        # x += 1
        end = time.time()
        if end - start >= sleep_duration:
            break
    return x


@python_app
def add_inc(inputs=[]):
    import time
    start = time.time()
    sleep_duration = 0
    #return inputs
    res = sum(inputs)
    while True:
        res += 1
        end = time.time()
        if end - start >= sleep_duration:
            break
    return res


if __name__ == "__main__":
    local_setup()
    total = 10
    half = int(total / 2)
    one_third = int(total / 3)
    two_third = int(total / 3 * 2)

    # fxc = FuncXClient()
    # task_uuid = fxc.register_function(inc)
    #print(task_uuid)
    #sys.exit()
    # app = python_app(inc)
    # futures_1 = [app(i) for i in range(total)]
    start = time.time()
    futures_1 = [inc(i) for i in range(total)]

    futures_2 = [add_inc(inputs=futures_1[0:half]),
                 add_inc(inputs=futures_1[half:total])]
    futures_3 = [inc(futures_2[0]) for _ in range(half)] + [inc(futures_2[1]) for _ in range(half)]
    futures_4 = [add_inc(inputs=futures_3[0:one_third]),
                 add_inc(inputs=futures_3[one_third:two_third]),
                 add_inc(inputs=futures_3[two_third:total])]

    print([f.result() for f in futures_1])
    print([f.result() for f in futures_4])
    print(f"Done in {time.time() - start}")
