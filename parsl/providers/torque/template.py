template_string = '''#!/bin/bash

${scheduler_options}
#PBS -S /bin/bash
#PBS -N ${jobname}
#PBS -m n
#PBS -l walltime=$walltime
#PBS -l nodes=${nodes_per_block}:ppn=${tasks_per_node}
#PBS -o ${submit_script_dir}/${jobname}.submit.stdout
#PBS -e ${submit_script_dir}/${jobname}.submit.stderr

${worker_init}

export JOBNAME="${jobname}"

${user_script}

'''
