#  _*_  coding : utf-8    _*_
#
#  Parsl DAG Application for strategy performance
#  based on app_dag_zhuzhao.py
#   Author: Takuya Kurihana 
#
import argparse
import numpy as np
import psutil
import parsl
from parsl import *
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels  import LocalChannel
from parsl.launchers import SingleNodeLauncher
import logging
from parsl.app.app import python_app

# Argument settings
p = argparse.ArgumentParser()
p.add_argument(
    "--executor",
    help="executor option for configuration setting ",
    type=str,
    default='HighThroughput'
)
args = p.parse_args()
print('Argparse:  --executor='+args.executor, flush=True)


# Here we can add htex_strategy for option
# config
print(type(args.executor) )
if args.executor == 'ThreadPool':
  config = Config(
      executors=[ThreadPoolExecutor(
          #label='threads',
          label='htex_local',
          max_threads=5)
      ],
  )
elif args.executor == 'HighThroughput':
  config = Config(
      executors=[
          HighThroughputExecutor(
            label="htex_local",
            cores_per_worker=1,
            provider=LocalProvider(
              channel=LocalChannel(),
              init_blocks=1,
              max_blocks=1,
              # tasks_per_node=1,  # For HighThroughputExecutor, this option sho<
              launcher=SingleNodeLauncher(),
            ),
        )
    ],
    #strategy='htex_aggressive',
    #strategy='htex_totaltime',
    strategy='simple',
  )
# TODO: 
#try:
#except:
#  raise NameError("Invalid parsed argument")  

# Load config
print(config)
dfk = parsl.load(config)


@App('python', dfk)
def sleeper(dur=5):
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

@python_app
def inc(inputs=[]):
    import time
    import psutil
    import numpy as np
    start = time.time()
    sleep_duration = 60.0
    _inputs = np.asarray(inputs)
    mems = _inputs[0].tolist()
    cpus = _inputs[1].tolist()
    x = 0
    while True:
        x += 1
        end = time.time()
        if (end - start) % 10 == 0:
            mems += [psutil.virtual_memory().percent]
            cpus += [psutil.cpu_percent()]
        if end - start >= sleep_duration:
            break
    mems += [psutil.virtual_memory().percent]
    cpus += [psutil.cpu_percent()]
    return [mems, cpus]

@python_app
def add_inc(inputs=[]):
    import time
    import psutil
    import numpy as np
    start = time.time()
    sleep_duration = 60.0
    res = 0
    _inputs = np.asarray(inputs)
    mems = _inputs[0].tolist()
    cpus = _inputs[1].tolist()
    while True:
        res += 1
        end = time.time()
        if (end - start) % 10 == 0:
            mems += [psutil.virtual_memory().percent]
            cpus += [psutil.cpu_percent()]
        if end - start >= sleep_duration:
            break
    mems += [psutil.virtual_memory().percent]
    cpus += [psutil.cpu_percent()]
    return  [mems, cpus]


if __name__ == "__main__":

    total = 10 
    half = int(total / 2)
    one_third = int(total / 3)
    two_third = int(total / 3 * 2)
    mems = [psutil.virtual_memory().percent]
    cpus = [psutil.cpu_percent()]
    inputs = [mems, cpus]
    futures_1 = [inc(inputs) for i in range(total)]
    futures_2 = [add_inc(inputs=futures_1[0:half]), add_inc(inputs=futures_1[half:total])]  
    futures_3 = [inc(futures_2[0]) for _ in range(half)] + [inc(futures_2[1]) for _ in range(half)]
    futures_4 = [add_inc(inputs=futures_3[0:one_third]), add_inc(inputs=futures_3[one_third:two_third]), add_inc(inputs=futures_3[two_third:total])]
    
    print([f.result() for f in futures_4])
    print("Done")

    # plotting
    outputs = np.asarray([f.result() for f in futures_4])
    print(outputs.shape)

    cdir='/home/tkurihana/scratch-midway2/parsl/parsl/dataflow/workspace'
    np.save(cdir+'/'+"outputs", outputs)
