# _*_  coding : utf-8   _*_

import sys
import time
import psutil
import logging
import matplotlib.pyplot as plt
# Log 
logger  = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.setLevel(logging.INFO) 

stime = time.time()

def _get_cpu(*arg):
  #cpu_percent = psutil.cpu_percent(interval=0)
  cpu_percent = psutil.cpu_percent()
  logging.info("  ### CPU Util : %f ### " % float(cpu_percent) )
  return float(cpu_percent)

def  _get_mem(*arg):
  memory = psutil.virtual_memory()
  logging.info("  ### MEM Util : %f ### " % float(memory.percent))
  return float(memory.percent)

# initial
mem_list = []
cpu_list = []
times_list = []
cpu_list.append(_get_cpu())
mem_list.append(_get_mem())
times_list.append(time.time()-stime)

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
for i in range(2):
  mem, cpu, times = func(n = _n*(i+1))
  mem_list.extend(mem)
  cpu_list.extend(cpu)
  times_list.extend(times)
  time.sleep(10*(i+1))
  cpu_list.append(_get_cpu())
  mem_list.append(_get_mem())
  times_list.append(time.time()-stime)

  
cpu_percent = psutil.cpu_percent(interval=0)
memory = psutil.virtual_memory()
logging.info("  ### CPU Util : %f ### " % float(cpu_percent) )
logging.info("  ### MEM Util : %f ### " % float(memory.percent))
cpu_list.append(_get_cpu())
mem_list.append(_get_mem())
times_list.append(time.time()-stime)

plt.figure()
plt.subplot(121)
plt.hist(mem_list, label='memory', color='blue', alpha=0.3)
plt.legend()
plt.subplot(122)
plt.hist(cpu_list, label='cpu',    color='red', alpha=0.3)
plt.legend()
plt.show()

plt.figure()
plt.subplot(121)
plt.plot(times_list, mem_list, label='memory', color='blue')
plt.legend()
plt.subplot(122)
plt.plot(times_list, cpu_list, label='cpu',    color='red')
plt.legend()
plt.show()
