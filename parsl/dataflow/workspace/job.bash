#!/bin/bash
#SBATCH --job-name=cpu_strs
#SBATCH --output=%x-%A.out
#SBATCH --error=%x-%A.err
#SBATCH -p broadwl
#SBATCH --time=0:10:00

module load Anaconda3/5.0.0.1
source activate py3501

python cpu_stress.py

