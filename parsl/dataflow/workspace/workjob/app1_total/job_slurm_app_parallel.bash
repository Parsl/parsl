#!/bin/bash
#SBATCH --job-name=App_Slurm
#SBATCH --output=%x-%A-%a.out
#SBATCH --error=%x-%A-%a.err
#SBATCH -p broadwl
#SBATCH --nodes=5
#SBATCH --time=12:00:00
###SBATCH --array 4
##SBATCH --array=1-3
#SBATCH --array=2

# Here you should modify for your own conda-env
module load Anaconda3/5.0.0.1
source activate parsl-dev

# modify config below program as following points
# - worker_init:  e.g.worker_init='module load Anaconda3/5.0.0.1; source activate py3501'
# - 

# Settings
cdir=`pwd`
if [ ${SLURM_ARRAY_TASK_ID} -eq 1 ]; then
  strategy=simple
fi
if [ ${SLURM_ARRAY_TASK_ID} -eq 2 ]; then
  strategy=htex_totaltime
fi
if [ ${SLURM_ARRAY_TASK_ID} -eq 3 ]; then
  strategy=htex_gradient
fi
if [ ${SLURM_ARRAY_TASK_ID} -eq 4 ]; then
  strategy=htex_aggressive
fi
oname=${SLURM_JOB_ID}-${strategy}

# Submit job
time python app_slurm_dag.py \
  --executor=HighThroughput_Slurm \
  --strategy=${strategy} \
  --oname=${oname}  \
  --ofiledir=${cdir} 

