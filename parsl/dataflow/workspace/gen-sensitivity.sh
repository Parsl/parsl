#!/bin/bash

# Run the app
#python test_cpu_stress.py

# Collect data
cat runinfo/${1}/parsl.log | grep "\[MONITOR\] active tasks" > task.data
cat runinfo/${1}/parsl.log | grep "\[MONITOR\] active slots" > slot.data

# Draw graph
python draw-strategy-sensitivity.py

rm task.data
rm slot.data


