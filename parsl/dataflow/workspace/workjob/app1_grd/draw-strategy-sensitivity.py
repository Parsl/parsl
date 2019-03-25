import os
import argparse
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# get dir for save file
cwd=os.getcwd()

# get parsed arguments
p = argparse.ArgumentParser()
p.add_argument(
  '--outdir',
  help='output file directory',
  type=str,
  default='.'
)
p.add_argument(
  '--outfilename',
  help='output file name',
  type=str,
  default='output-'+datetime.now().strftime("%m%d%H%M%S")
)
args = p.parse_args()

# files
task_path = 'task.data'
slot_path = 'slot.data'

tasks = []
slots = []

# Read task data points
with open(task_path) as f:
    lines = f.readlines()
    for line in lines:
        line = line.split(" ")
        task = int(line[len(line)-1])
        tasks.append(task)

with open(slot_path) as f:
    lines = f.readlines()
    for line in lines:
        line = line.split(" ")
        slot = int(line[len(line)-1])
        slots.append(slot)

print(tasks)
print(slots)
limit = max(max(tasks), max(slots)) + 10
time = range(len(slots))
# Draw graph
fig, ax1 = plt.subplots()
ax1.plot(time, tasks, 'b-', linewidth=2)
ax1.set_xlabel('Time steps', fontsize=16)
# Make the y-axis label, ticks and tick labels match the line color.
ax1.set_ylabel('# Tasks', color='b', fontsize=16)
ax1.tick_params('y', colors='b', labelsize=16)
ax1.set_ylim(0, limit)

ax2 = ax1.twinx()
ax2.plot(time, slots, 'r--', linewidth=2)
#ax2.set_ylabel('# Slots', color='r')
ax2.set_ylabel('# Cores', color='r', fontsize=16)
ax2.tick_params('y', colors='r', labelsize=16)
ax2.set_ylim(0, limit)

fig.tight_layout()
#plt.show()
plt.savefig(args.outdir+'/'+args.outfilename)
print("##  NORMAL END ##", flush=True)



