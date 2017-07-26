template_string = '''#!/bin/bash

#SBATCH --job-name=$jobname
#SBATCH --output=$submit_script_dir/parsl-$jobname.submit.stdout
#SBATCH --error=$submit_script_dir/parsl-$jobname.submit.stderr
#SBATCH --nodes=$nodes
#SBATCH --partition=$queue
#SBATCH --time=$walltime
#SBATCH --ntasks-per-node=$tasks_per_node
#SBATCH --account=$account
$slurm_overrides

$user_script
'''
