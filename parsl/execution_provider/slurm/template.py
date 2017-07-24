template_string = '''#!/bin/bash

#SBATCH --job-name=$jobname
#SBATCH --output=$submit_dir/parsl-$job_id.submit.stdout
#SBATCH --error=$submit_dir/parsl-$job_id.submit.stderr
#SBATCH --nodes=$nodes
#SBATCH --partition=$queue
#SBATCH --time=$walltime
#SBATCH --ntasks-per-node=$tasks_per_node
#SBATCH --cpus-per-task=$cpus_per_task
#SBATCH --account=$account
$slurm_overrides

$user_script
'''
