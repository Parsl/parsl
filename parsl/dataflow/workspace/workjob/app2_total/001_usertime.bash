#!/bin/bash

jobid=${1}
jobtype=${2}
cwd=`pwd` 

if  [ -f ${cwd}/${jobtype}.out  ]; then 
  grep 'user'  ./App*${jobid}*err  >> ${jobtype}.out
fi
if  [ ! -f ${cwd}/${jobtype}.out  ]; then 
  grep 'user'  ./App*${jobid}*err  >  ${jobtype}.out
fi

echo NORMAL END
