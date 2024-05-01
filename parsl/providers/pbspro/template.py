template_string = '''#!/bin/bash

#PBS -S /bin/bash
#PBS -N ${jobname}
#PBS -m n
#PBS -l walltime=$walltime
#PBS -l select=${nodes_per_block}:ncpus=${ncpus}${select_options}
#PBS -o ${job_stdout_path}
#PBS -e ${job_stderr_path}
${scheduler_options}

${worker_init}

export JOBNAME="${jobname}"

${user_script}

'''
