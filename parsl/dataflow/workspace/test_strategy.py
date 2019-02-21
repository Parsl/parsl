# _*_ coding: utf-8 _*_

import os
import sys
sys.path.insert(0,"../" )
from strategy import Strategy

def foo(*arg):
  print("Hello Our New Strategy")
  return True


stg = Strategy(foo)

tasks = [1]
stg._htex_strategy_totaltime(tasks)
