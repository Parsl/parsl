#!/bin/bash
#SBATCH --job-name=app_slurm
#SBATCH --output=%x-%A.out
#SBATCH --error=%x-%A.err
#SBATCH -p broadwl
#SBATCH --nodes=5
#SBATCH --time=0:60:00


# Here you should modify for your own conda-env
module load Anaconda3/5.0.0.1
source activate parsl-dev

# modify config below program as following points
# - worker_init:  e.g.worker_init='module load Anaconda3/5.0.0.1; source activate py3501'
# - 
cdir=`pwd`
oname=$SLURM_JOB_ID
python app_slurm_dag_zhuozhao.py  --executor=HighThroughput_Slurm --strategy=simple --oname=${oname} --ofiledir=${cdir} 

