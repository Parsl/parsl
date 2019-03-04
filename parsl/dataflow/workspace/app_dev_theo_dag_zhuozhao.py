#  _*_  coding : utf-8    _*_
#
#  Parsl DAG Application for strategy performance
#  based on app_dag_zhuzhao.py
#   Author: Takuya Kurihana 
#
import parsl
from parsl import *
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
import logging
from parsl.app.app import python_app

# Here we can add htex_strategy for option
threads_config = Config(
    executors=[ThreadPoolExecutor(
        #label='threads',
        label='htex_local',
        max_threads=5)
    ],
)

dfk = parsl.load(threads_config)

@App('python', dfk)
def sleeper(dur=5):
    import time
    time.sleep(dur)


@App('python', dfk)
def cpu_stress(dur=30):
    import time
    s = 0
    start = time.time()
    while True:
        s += i
        if time.time() - start >= dur:
            break
    return s

@python_app
def inc(x):
    import time
    start = time.time()
    sleep_duration = 60.0
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
    sleep_duration = 60.0
    res = sum(inputs)
    while True:
        res += 1
        end = time.time()
        if end - start >= sleep_duration:
            break
    return res


if __name__ == "__main__":

    total = 10 
    half = int(total / 2)
    one_third = int(total / 3)
    two_third = int(total / 3 * 2)
    futures_1 = [inc(i) for i in range(total)]
    futures_2 = [add_inc(inputs=futures_1[0:half]), add_inc(inputs=futures_1[half:total])]  
    futures_3 = [inc(futures_2[0]) for _ in range(half)] + [inc(futures_2[1]) for _ in range(half)]
    futures_4 = [add_inc(inputs=futures_3[0:one_third]), add_inc(inputs=futures_3[one_third:two_third]), add_inc(inputs=futures_3[two_third:total])]
    
    print([f.result() for f in futures_4])
    print("Done")
