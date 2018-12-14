template_string = """#!/bin/bash
#$$ -S /bin/bash
#$$ -o ${submit_script_dir}/${jobname}.submit.stdout
#$$ -e ${submit_script_dir}/${jobname}.submit.stderr
#$$ -cwd
#$$ -l h_rt=${walltime}
${scheduler_options}

${worker_init}

export JOBNAME="${jobname}"

$user_script
"""
