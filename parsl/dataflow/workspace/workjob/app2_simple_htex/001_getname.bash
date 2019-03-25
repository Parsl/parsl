#!/bin/bash

#App_Slurm-58806759-4.out

filelists=`pwd`/"App_Slurm2-*-4.out"
for ifile in ${filelists}; do
  #echo $ifile
  bname=`basename ${ifile}`
  a=${bname:11:8}
  echo ${a}
done
