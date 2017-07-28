template_string = '''#!/bin/bash

#SBATCH --job-name=$jobname
#SBATCH --output=$submit_script_dir/$jobname.submit.stdout
#SBATCH --error=$submit_script_dir/$jobname.submit.stderr
#SBATCH --nodes=$nodes
#SBATCH --partition=$queue
#SBATCH --time=$walltime
#SBATCH --ntasks-per-node=$tasks_per_node
#SBATCH --account=$account
$slurm_overrides

$user_script
'''
