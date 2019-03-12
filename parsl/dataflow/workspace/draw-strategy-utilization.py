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

apps = {}
app1 = {}
#app1['baseline'] = 'app1_simple'
app1['totaltime'] = 'app1_total'
app1['gradient'] = 'app1_grd'
app2 = {}
#app2['baseline'] = 'app2_simple'
app2['totaltime'] = 'app2_total'
app2['gradient'] = 'app2_grd'

apps['app1'] = app1
apps['app2'] = app2


# files
task_path = 'task.data'
slot_path = 'slot.data'

for name, app in apps.items():
    
    means = []
    upperbounds = []
    lowerbounds = []
    
    # Read task data points
    for stra, path in app.items():
        tasks = []
        slots = []
        
        with open(path + '-' + task_path) as f:
            lines = f.readlines()
            task = 0
            for line in lines:
                if ('END' in line):
                    tasks.append(task)
                    task = 0
                else:
                    line = line.split(" ")
                    task += int(line[len(line)-1])
    
        with open(path + '-' + slot_path) as f:
            lines = f.readlines()
            slot = 0
            for line in lines:
                if ('END' in line):
                    slots.append(slot)
                    slot = 0
                else:
                    line = line.split(" ")
                    slot += int(line[len(line)-1])
        
        print(tasks)
        print(slots)
        
        utilization = np.array(tasks) / np.array(slots) * 100
        means.append(np.average(utilization))
        upperbounds.append(np.percentile(utilization, 95))
        lowerbounds.append(np.percentile(utilization, 5))
    
    means = np.array(means)
    upperbounds = np.array(upperbounds)
    lowerbounds = np.array(lowerbounds)
    
    fig = plt.figure()
    error_bar_set = dict(lw = 1, capthick = 1, capsize = 14)
    plt.bar(np.arange(len(means)), means, yerr=np.vstack([means - lowerbounds, upperbounds - means]),
            error_kw=error_bar_set, 
            color='red', alpha=0.4)
    
    plt.ylabel("Utilization % ", fontsize=14)
    plt.ylim(0,110)
    plt.xticks(np.arange(len(means)), list(app.keys()))
    plt.tick_params(labelsize=14)
    plt.title(name + ' Utilization', fontsize=14)
    plt.savefig(name + "_cpu_utils.png")
        
plt.show()
plt.savefig(args.outdir+'/'+args.outfilename)
print("##  NORMAL END ##", flush=True)

# ++ Original script
# Draw graph
#fig, ax1 = plt.subplots()
#ax1.plot(time, tasks, 'b-')
#ax1.set_xlabel('time')
# Make the y-axis label, ticks and tick labels match the line color.
#ax1.set_ylabel('# Tasks', color='b')
#ax1.tick_params('y', colors='b')

#ax2 = ax1.twinx()
#ax2.plot(time, slots, 'r--')
#ax2.set_ylabel('# Slots', color='r')
#ax2.tick_params('y', colors='r')

