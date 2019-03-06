#!/bin/bash

# Run the app
#python test_cpu_stress.py
# parsed job-id for process
filename=./App_Slurm-${1}-*.out
stype=${2} # strategy tpe: simple/totaltile/gradient
echo ${filename}

# Collect data
cat $filename | grep "\[MONITOR\] Active tasks" > task.data
cat $filename | grep "\[MONITOR\] Active slots" > slot.data

# Figure Directory
figdir=`pwd`/fig 
mkdir -p $figdir

# Draw graph
python draw-strategy-sensitivity.py --outdir=${figdir} --outfilename=${stype}_${1}

rm task.data
rm slot.data


