# _*_  coding : utf-8   _*_
#
#   Multi-Core Test Script for strategy(ies) benchmark on slurm
#
__author__ = 'tkurihana@uchicago.edu'

import os
import sys
import time
from mod_libmonitors import _get_cpu, _get_mem

# parsl module 
import parsl
from parsl.app.app import python_app, bash_app
from parsl.launchers import SingleNodeLauncher
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_hostname
from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider
from parsl.launchers import SrunLauncher


# Log 
import logging
logger  = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.setLevel(logging.INFO) 

@python_app
def _logger(*args):
  import logging
  logger  = logging.getLogger()
  handler = logging.StreamHandler(sys.stdout)
  handler.setLevel(logging.INFO)
  logger.addHandler(handler)
  _logging = logger.setLevel(logging.INFO) 
  return _logging

# config
config = Config(
    executors=[
        HighThroughputExecutor(
            label="midway_htex",
            cores_per_worker=1,
            address=address_by_hostname(),
            provider=SlurmProvider(
                'broadwl',    # machine name on midway
                init_blocks=1,
                launcher=SrunLauncher(),
                scheduler_options='#SBATCH --exclusive',
                max_blocks=10,
                nodes_per_block=10,
                # tasks_per_node=1,  # For HighThroughputExecutor, this option sho<
                parallelism=1.0,
                walltime='00:10:00',
                worker_init='module load Anaconda3/5.0.0.1; source activate py3501'
            ),
        )
    ],
    #strategy='htex_aggressive',
    #strategy='htex_totaltime',
    strategy='simple',
)
# Load config 
parsl.load(config)


@python_app
def func(n=1000000, stime=0.00):
  # import necessary library?! otherwise get errors
  import os, sys, time
  import psutil
  import logging
  logger  = logging.getLogger()
  handler = logging.StreamHandler(sys.stdout)
  handler.setLevel(logging.INFO)
  logger.addHandler(handler)
  logger.setLevel(logging.INFO) 

  logging.info("  ### JOB START ### ")
  x = 0.0
  #n = 1000000
  num = 0
  mems = []
  cpus = []
  times = []
  while num < 1:
    init_time = time.time()
    for i in range(n):
      x += float(i)
      if i % 1000 == 0:
        print("   ", flush=True)
        mems += [psutil.virtual_memory().percent]
        cpus += [psutil.cpu_percent()]
        ctime = time.time() - stime
        times += [ctime]

    num = 1
    end_time  = time.time() - init_time
  logging.info("Elapse Time: %f" % end_time)
  # log end-time info
  mems += [psutil.virtual_memory().percent]
  cpus += [psutil.cpu_percent()]
  ctime = time.time() - stime
  times += [ctime]
  return mems, cpus, times


_n = 10000000
# initial
stime = time.time()
mem_list = []
cpu_list = []
times_list = []

rand_nums = []
cpu_list.append(_get_cpu())
mem_list.append(_get_mem())
times_list.append(time.time()-stime)
time.sleep(.100)
for i in range(4):
  rand_nums.append(func(_n, stime))
  #mem, cpu, times = func(_n, stime).result()

_outputs = [i.result() for i in rand_nums]
print("   Output Shape Check (#iteration, #variable, #log)" , np.asarray(_outputs).shape)

# update
outputs = np.asarray(_outputs)
mem = outputs[:,0,:]
cpu = outputs[:,1,:]
times = outputs[:,2,:]

# log list
mem_list.extend(mem.flatten())
cpu_list.extend(cpu.flatten())
times_list.extend(times.flatten())

logging.info("    #### NORMAL END  ####")
