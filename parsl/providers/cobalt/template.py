template_string = '''#!/bin/bash -el
${scheduler_options}

${worker_init}

echo "Starting Cobalt job script"

echo "----Cobalt Nodefile: -----"
cat $$COBALT_NODEFILE
echo "--------------------------"

export JOBNAME="${jobname}"

$user_script

echo "End of Cobalt job"
'''
