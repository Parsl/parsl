# _*_  coding : utf-8   _*_

import sys
import time
import psutil
import logging

import parsl


# Log 
logger  = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.setLevel(logging.INFO) 


def _get_cpu(*arg):
  #cpu_percent = psutil.cpu_percent(interval=0)
  cpu_percent = psutil.cpu_percent()
  logging.info("  ### CPU Util : %f ### " % float(cpu_percent) )
  return float(cpu_percent)

def  _get_mem(*arg):
  memory = psutil.virtual_memory()
  logging.info("  ### MEM Util : %f ### " % float(memory.percent))
  return float(memory.percent)


def func(n=1000000):
  logging.info("  ### JOB START ###")
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
        mems += [_get_cpu()]
        cpus += [_get_mem()]
        ctime = time.time() - stime
        times += [ctime]

    num = 1
    end_time  = time.time() - init_time
  logging.info("Elapse Time: %f" % end_time)
  return mems, cpus, times


_n = 10000000
# initial
stime = time.time()
mem_list = []
cpu_list = []
times_list = []
for i in range(2):
  time.sleep(10*(i+1))
  cpu_list.append(_get_cpu())
  mem_list.append(_get_mem())
  times_list.append(time.time()-stime)

  mem, cpu, times = func(n = _n*(i+1))
  
  # Upadate 
  mem_list.extend(mem)
  cpu_list.extend(cpu)
  times_list.extend(times)
   
