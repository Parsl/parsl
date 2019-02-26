
import sys
import psutil
import logging

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


