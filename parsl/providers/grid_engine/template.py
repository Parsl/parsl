template_string = """#!/bin/bash
#$$ -S /bin/bash
#$$ -o ${submit_script_dir}/${jobname}.submit.stdout
#$$ -e ${submit_script_dir}/${jobname}.submit.stderr
#$$ -cwd
#$$ -l h_rt=${walltime}
$overrides

export JOBNAME="${jobname}"

$user_script
"""
