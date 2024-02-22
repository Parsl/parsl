template_string = '''#!/bin/bash

#SBATCH --job-name=${jobname}
#SBATCH --output=${job_stdout_path}
#SBATCH --error=${job_stderr_path}
#SBATCH --nodes=${nodes}
#SBATCH --time=${walltime}
#SBATCH --ntasks-per-node=${tasks_per_node}
${scheduler_options}

${worker_init}

export JOBNAME="${jobname}"

$user_script
'''
