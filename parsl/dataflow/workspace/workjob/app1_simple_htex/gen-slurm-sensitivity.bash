#!/bin/bash

# Run the app
#python test_cpu_stress.py
# parsed job-id for process
filelists=`pwd`/"App_Slurm-*-4.out"
for ifile in ${filelists}; do
  #echo $ifile
  bname=`basename ${ifile}`
  fileid=${bname:10:8}
  echo ${fileid}

  filename=./App_Slurm*-${fileid}-*.out
  stype=${1} # strategy tpe: simple/totaltile/gradient
  echo ${filename}

  # Collect data
  cat $filename | grep "\[MONITOR\] Active tasks" > task.data
  cat $filename | grep "\[MONITOR\] Active slots" > slot.data

  # Figure Directory
  figdir=`pwd`/fig 
  mkdir -p $figdir

  # Draw graph
  python draw-strategy-sensitivity.py --outdir=${figdir} --outfilename=${stype}_${fileid}

  rm task.data
  rm slot.data

done
