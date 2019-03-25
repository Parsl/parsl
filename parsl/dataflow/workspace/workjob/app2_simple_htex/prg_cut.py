# _*_ coding : utf-8  _*_

import os
import sys
import numpy as np
import glob

datadir=os.getcwd()

filelist = glob.glob(datadir+'/outputs_app2-1_slurm-*.npy')
filelist.sort()

outlist = glob.glob(datadir+'/App_Slurm*.out')
outlist.sort()

#for  inpy, jout in zip(  filelist , outlist ):
#    ijob =  os.path.basename(inpy)[20:28]
#    jjob =  os.path.basename(jout)[10:18]
#    print(ijob)
#    print(jjob)

jobnamelist = []
for  inpy in filelist:
    jobnamelist += [os.path.basename(inpy)[21:29] ]

for iout in outlist:
  ijob = os.path.basename(iout)[11:19]
  if ijob in jobnamelist:
    print(ijob)
  else:
    print("  ###  Remove  %s  ###" %  str(ijob))
    os.remove(iout)
