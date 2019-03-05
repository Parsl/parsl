import numpy as np
import matplotlib.pyplot as plt

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
time = range(len(slots))
# Draw graph
fig, ax1 = plt.subplots()
ax1.plot(time, tasks, 'b-')
ax1.set_xlabel('time')
# Make the y-axis label, ticks and tick labels match the line color.
ax1.set_ylabel('# Tasks', color='b')
ax1.tick_params('y', colors='b')

ax2 = ax1.twinx()
ax2.plot(time, slots, 'r--')
ax2.set_ylabel('# Slots', color='r')
ax2.tick_params('y', colors='r')

fig.tight_layout()
plt.show()



