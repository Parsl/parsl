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
from parsl.addresses import address_by_hostname
from parsl.channels  import SSHChannel
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher
import logging
from parsl.app.app import python_app

# Argument settings
p = argparse.ArgumentParser()
# Executor option
p.add_argument(
    "--executor",
    help="executor option for configuration setting ",
    type=str,
    default='HighThroughput_Slurm'
)
# outputfilename
p.add_argument(
    "--oname",
    help="output npy-filename option ",
    type=str,
    default='change'
)
args = p.parse_args()
args = p.parse_args()
print('Argparse:  --executor='+args.executor, flush=True)


# Here we can add htex_strategy for option
# config
if args.executor == 'ThreadPool':
  config = Config(
      executors=[ThreadPoolExecutor(
          #label='threads',
          label='htex_local',
          max_threads=5)
      ],
  )
elif args.executor == 'HighThroughput_Local':
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
elif args.executor == 'HighThroughput_Slurm':
  config = Config(
    executors=[
        HighThroughputExecutor(
            label="midway_htex",
            cores_per_worker=1,
            address=address_by_hostname(),
            provider=SlurmProvider(
                #'broadwl',    # machine name on midway
                'build',    # machine name on midway
                launcher=SrunLauncher(),
                scheduler_options='#SBATCH --mem-per-cpu=16000 ',
                ###scheduler_options='#SBATCH --exclusive',
                worker_init='module load Anaconda3/5.0.0.1; source activate parsl',
                init_blocks=1,
                max_blocks=1,
                nodes_per_block=1,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option sho<
                parallelism=1.0,
                walltime='00:10:00',
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
    mems = [] #_inputs[0].tolist()
    cpus = [] #_inputs[1].tolist()
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
    mems = [] # _inputs[0].tolist()
    cpus = [] # _inputs[1].tolist()
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
    _outputs1 = [ i.result() for i in futures_1]
    futures_2 = [add_inc(inputs=futures_1[0:half]), add_inc(inputs=futures_1[half:total])]  
    _outputs2 = [ i.result() for i in futures_2]
    futures_3 = [inc(futures_2[0]) for _ in range(half)] + [inc(futures_2[1]) for _ in range(half)]
    _outputs3 = [ i.result() for i in futures_3]
    futures_4 = [add_inc(inputs=futures_3[0:one_third]), add_inc(inputs=futures_3[one_third:two_third]), add_inc(inputs=futures_3[two_third:total])]
    _outputs4 = [ i.result() for i in futures_4]
    
    print([f.result() for f in futures_4])
    print("Done")

    # plotting
    for iout in [_outputs1, _outputs2, _outputs3, _outputs4]:
        _iout = np.asarray(iout)
        print(_iout.shape)
        print(_iout)
        _mem  = _iout[:,0].flatten() 
        _cpu  = _iout[:,1].flatten()
        mems.extend(_mem)
        cpus.extend(_cpu)
    
    print(mems)
    print()
    print(cpus)
    
    home='/home/tkurihana/scratch-midway2/'
    home='/home/ndhai/home/sources/'
    cdir = home + 'parsl/parsl/dataflow/workspace/runinfo/slurm/'
    np.save(cdir+'/'+"output_mems_slurm-"+args.oname, np.asarray(mems))
    np.save(cdir+'/'+"output_cpus_slurm-"+args.oname, np.asarray(cpus))
