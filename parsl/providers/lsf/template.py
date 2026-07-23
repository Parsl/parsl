template_string = '''#!/bin/bash

#BSUB -W ${walltime}
#BSUB -J ${jobname}
#BSUB -cwd ${submit_script_dir}
#BSUB -o ${jobname}.submit.stdout
#BSUB -e ${jobname}.submit.stderr
${scheduler_options}

${worker_init}

export JOBNAME="${jobname}"

$user_script
'''
