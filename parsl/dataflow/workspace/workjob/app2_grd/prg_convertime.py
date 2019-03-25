# _*_ coding : utf-8  _*_
import os
import datetime
import argparse
import numpy as np

def get_runtime(string):
  # convert string to time
  mdx = string.find('m')  # index of minutes
  minutes = float(string[:mdx])*60
  seconds = float(string[mdx+1:-2])
  return minutes+seconds
  
def _main(filedir='.', filename='xxx',
          outputdir='.', oname='ofile', 
          fprint=False
  ):
  runs = []
  with open(filedir+'/'+filename) as ifile:
    lines = ifile.readlines()
    for line in lines:
      line = line.replace("\t", "")
      line = line.replace("user", "")
      line = line.replace("'", "")
      runtime = get_runtime(line)
      if fprint:
        print(line)
        print('convert time (sec): %f' % runtime)
      runs.append(runtime)
  np.save(outputdir+'/'+oname,np.asarray(runs) )
  sname = outputdir+'/'+oname+'.npy'
  print(" ## File Saved : %s ## " %sname)

if __name__ == "__main__":
  # argument
  p = argparse.ArgumentParser()
  p.add_argument('--filedir', default='.')
  p.add_argument('--filename')
  p.add_argument('--outputdir', default='.')
  p.add_argument('--oname')
  args = p.parse_args()
  
  _main(filedir=args.filedir,
        filename=args.filename,
        outputdir=args.outputdir,
        oname=args.oname
        )
