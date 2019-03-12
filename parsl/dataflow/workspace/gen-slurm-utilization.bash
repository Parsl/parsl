#!/bin/bash

# Remember to copy the file to workjob directory.

declare -a dicts=("app1_grd" "app1_simple" "app1_total" "app2_grd" "app2_simple" "app2_total")

for file in "${dicts[@]}"
do
	task=${file}-task.data
	slot=${file}-slot.data
	touch $task
	touch $slot
	for filename in $file/*.out
	do
		echo $filename
		# Collect data
		cat $filename | grep "\[MONITOR\] Active tasks" >> $task
		echo "END" >> $task
		cat $filename | grep "\[MONITOR\] Active slots" >> $slot
		echo "END" >> $slot
	done
done

# Draw graph
python draw-strategy-utilization.py

rm *-task.data
rm *-slot.data


