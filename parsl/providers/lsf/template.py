template_string = '''#!/bin/bash

#BSUB -W ${walltime}
#BSUB -J ${jobname}
#BSUB -o ${submit_script_dir}/${jobname}.submit.stdout
#BSUB -e ${submit_script_dir}/${jobname}.submit.stderr
${scheduler_options}

${worker_init}

export JOBNAME="${jobname}"

$user_script
'''
